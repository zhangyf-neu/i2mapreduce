/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.GlobalUniqKeyWritable;
import org.apache.hadoop.io.GlobalUniqValueWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapred.IFile.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Merger.Segment;
import org.apache.hadoop.mapred.PreserveMerger.KVSSegment;
import org.apache.hadoop.mapred.SortedRanges.SkipRangeIterator;
import org.apache.hadoop.mapred.TaskTracker.TaskInProgress;
import org.apache.hadoop.mapred.IterativeTaskCompletionEvent;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.metrics2.MetricsBuilder;
import org.apache.hadoop.util.PriorityQueue;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapreduce.security.SecureShuffleUtils;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricMutableCounterInt;
import org.apache.hadoop.metrics2.lib.MetricMutableCounterLong;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;

/** A Reduce task. */
class ReduceTask extends Task {

	private class MapOutputReadyChecker extends Thread {

		private TaskUmbilicalProtocol trackerUmbilical;

		private ReduceTask reducetask;
		
		private int iteration;

		public MapOutputReadyChecker(TaskUmbilicalProtocol umbilical,
				ReduceTask reducetask, int iteration) {
			this.trackerUmbilical = umbilical;
			this.reducetask = reducetask;
			this.iteration = iteration;
		}

		public void run() {
			while (!isInterrupted()/*
									 * && finishedReduceTasks.size() <
									 * getNumberOfInputs()+1
									 */) {
				try {
					synchronized (reducetask) {
						LOG.info("mapoutput ready check!");
						MapReduceOutputReadyEvent mapreadyevent = trackerUmbilical
								.getMapReduceOutputReadyEvent(getJobID(),
										iteration, -1);

						if (mapreadyevent != null) {
							LOG.info("iteration " + iteration
									+ " mapreadyevent is not null!");
							if (mapreadyevent.isMapOutputReady()) {
								LOG.info("iteration " + iteration
										+ " Map output is ready!");
								reducetask.notifyAll();
								iteration++;
							}
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				}

				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					return;
				}
			}
		}
	}

	static { // register a ctor
		WritableFactories.setFactory(ReduceTask.class, new WritableFactory() {
			public Writable newInstance() {
				return new ReduceTask();
			}
		});
	}

	public static final boolean IsPreserveMore = false;

	private static final Log LOG = LogFactory
			.getLog(ReduceTask.class.getName());
	private int numMaps;
	private ReduceCopier reduceCopier;

	private CompressionCodec codec;

	{
		getProgress().setStatus("reduce");
		setPhase(TaskStatus.Phase.SHUFFLE); // phase to start with
	}

	private Progress copyPhase;
	private Progress sortPhase;
	private Progress reducePhase;
	private Counters.Counter reduceShuffleBytes = getCounters().findCounter(
			Counter.REDUCE_SHUFFLE_BYTES);
	private Counters.Counter reduceInputKeyCounter = getCounters().findCounter(
			Counter.REDUCE_INPUT_GROUPS);
	private Counters.Counter reduceInputValueCounter = getCounters()
			.findCounter(Counter.REDUCE_INPUT_RECORDS);
	private Counters.Counter reduceOutputCounter = getCounters().findCounter(
			Counter.REDUCE_OUTPUT_RECORDS);
	private Counters.Counter reduceCombineOutputCounter = getCounters()
			.findCounter(Counter.COMBINE_OUTPUT_RECORDS);

	// A custom comparator for map output files. Here the ordering is determined
	// by the file's size and path. In case of files with same size and
	// different
	// file paths, the first parameter is considered smaller than the second
	// one.
	// In case of files with same size and path are considered equal.
	private Comparator<FileStatus> mapOutputFileComparator = new Comparator<FileStatus>() {
		public int compare(FileStatus a, FileStatus b) {
			if (a.getLen() < b.getLen())
				return -1;
			else if (a.getLen() == b.getLen())
				if (a.getPath().toString().equals(b.getPath().toString()))
					return 0;
				else
					return -1;
			else
				return 1;
		}
	};

	// A sorted set for keeping a set of map output files on disk
	private final SortedSet<FileStatus> mapOutputFilesOnDisk = new TreeSet<FileStatus>(
			mapOutputFileComparator);

	public ReduceTask() {
		super();
	}

	public ReduceTask(String jobFile, TaskAttemptID taskId, int partition,
			int numMaps, int numSlotsRequired) {
		super(jobFile, taskId, partition, numSlotsRequired);
		this.numMaps = numMaps;
	}

	private CompressionCodec initCodec() {
		// check if map-outputs are to be compressed
		if (conf.getCompressMapOutput()) {
			Class<? extends CompressionCodec> codecClass = conf
					.getMapOutputCompressorClass(DefaultCodec.class);
			return ReflectionUtils.newInstance(codecClass, conf);
		}

		return null;
	}

	@Override
	public TaskRunner createRunner(TaskTracker tracker, TaskInProgress tip,
			TaskTracker.RunningJob rjob) throws IOException {
		return new ReduceTaskRunner(tip, tracker, this.conf, rjob);
	}

	@Override
	public boolean isMapTask() {
		return false;
	}

	public int getNumMaps() {
		return numMaps;
	}

	/**
	 * Localize the given JobConf to be specific for this task.
	 */
	@Override
	public void localizeConfiguration(JobConf conf) throws IOException {
		super.localizeConfiguration(conf);
		conf.setNumMapTasks(numMaps);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);

		out.writeInt(numMaps); // write the number of maps
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);

		numMaps = in.readInt();
	}

	// Get the input files for the reducer.
	private Path[] getMapFiles(FileSystem fs, boolean isLocal)
			throws IOException {
		List<Path> fileList = new ArrayList<Path>();
		if (isLocal) {
			// for local jobs
			for (int i = 0; i < numMaps; ++i) {
				fileList.add(mapOutputFile.getInputFile(i));
			}
		} else {
			// for non local jobs
			for (FileStatus filestatus : mapOutputFilesOnDisk) {
				fileList.add(filestatus.getPath());
			}
		}
		return fileList.toArray(new Path[0]);
	}

	private class ReduceValuesIterator<KEY, VALUE> extends
			ValuesIterator<KEY, VALUE> {
		public ReduceValuesIterator(RawKeyValueIterator in,
				RawComparator<KEY> comparator, Class<KEY> keyClass,
				Class<VALUE> valClass, Configuration conf, Progressable reporter)
				throws IOException {
			super(in, comparator, keyClass, valClass, conf, reporter);
		}

		@Override
		public VALUE next() {
			reduceInputValueCounter.increment(1);
			return moveToNext();
		}

		protected VALUE moveToNext() {
			return super.next();
		}

		public void informReduceProgress() {
			reducePhase.set(super.in.getProgress().get()); // update progress
			reporter.progress();
		}
	}

	private class PreserveReduceValuesIterator<KEY, VALUE, SOURCEKEY, OUTVALUE>
			extends SourceValuesIterator<KEY, VALUE, SOURCEKEY, OUTVALUE> {
		public PreserveReduceValuesIterator(
				RawKeyValueSourceIterator in,
				RawComparator<KEY> comparator,
				Class<KEY> keyClass,
				Class<VALUE> valClass,
				Class<SOURCEKEY> skeyClass,
				Class<OUTVALUE> outvalueClass,
				IFile.PreserveFile<KEY, VALUE, SOURCEKEY, OUTVALUE> preserveWriter,
				Configuration conf, Progressable reporter) throws IOException {
			super(in, comparator, keyClass, valClass, skeyClass, outvalueClass,
					preserveWriter, getTaskID().getTaskID().getId(), conf,
					reporter);
		}

		@Override
		public VALUE next() {
			reduceInputValueCounter.increment(1);
			return moveToNext();
		}

		protected VALUE moveToNext() {
			return super.next();
		}

		public void informReduceProgress() {
			reducePhase.set(super.in.getProgress().get()); // update progress
			reporter.progress();
		}
	}

	static interface IncrementalSourceValuesIterator<KEY, VALUE, SOURCEKEY, OUTVALUE>
			extends Iterator<VALUE> {

		boolean hasNext();

		VALUE next();

		void nextKey() throws IOException;

		boolean more();

		KEY getKey();

		void close() throws IOException;

		OUTVALUE getPreservedOutValue();

		void updateResKV(KEY key, OUTVALUE outvalue) throws IOException;
	}

	static class SourceValuesIterator<KEY, VALUE, SOURCEKEY, OUTVALUE>
			implements Iterator<VALUE> {
		protected RawKeyValueSourceIterator in; // input iterator
		private KEY key; // current key
		private KEY nextKey;
		private VALUE value; // current value
		private SOURCEKEY skey; // current value
		private boolean hasNext; // more w/ this key
		private boolean more; // more in file
		private RawComparator<KEY> comparator;
		protected Progressable reporter;
		private Deserializer<KEY> keyDeserializer;
		private Deserializer<VALUE> valDeserializer;
		private Deserializer<SOURCEKEY> skeyDeserializer;
		private DataInputBuffer keyIn = new DataInputBuffer();
		private DataInputBuffer valueIn = new DataInputBuffer();
		private DataInputBuffer skeyIn = new DataInputBuffer();
		private IFile.PreserveFile<KEY, VALUE, SOURCEKEY, OUTVALUE> preserveWriter;

		public SourceValuesIterator(
				RawKeyValueSourceIterator in,
				RawComparator<KEY> comparator,
				Class<KEY> keyClass,
				Class<VALUE> valClass,
				Class<SOURCEKEY> skeyClass,
				Class<OUTVALUE> outvalueClass,
				IFile.PreserveFile<KEY, VALUE, SOURCEKEY, OUTVALUE> preserveWriter,
				int taskid, Configuration conf, Progressable reporter)
				throws IOException {
			this.in = in;
			this.comparator = comparator;
			this.reporter = reporter;
			this.preserveWriter = preserveWriter;
			SerializationFactory serializationFactory = new SerializationFactory(
					conf);
			this.keyDeserializer = serializationFactory
					.getDeserializer(keyClass);
			this.keyDeserializer.open(keyIn);
			this.valDeserializer = serializationFactory
					.getDeserializer(valClass);
			this.valDeserializer.open(this.valueIn);
			this.skeyDeserializer = serializationFactory
					.getDeserializer(skeyClass);
			this.skeyDeserializer.open(this.skeyIn);
			readNextKey();
			key = nextKey;
			nextKey = null; // force new instance creation
			hasNext = more;
		}

		RawKeyValueIterator getRawIterator() {
			return in;
		}

		// / Iterator methods

		public boolean hasNext() {
			return hasNext;
		}

		private int ctr = 0;

		public VALUE next() {
			if (!hasNext) {
				throw new NoSuchElementException("iterate past last value");
			}
			try {
				readNextValue();
				readNextSource();

				preserveWriter.appendShuffleKVS(key, value, skey);
				// LOG.info("appendShuffleKVS: key:   "+key + "\tskey:  " + skey
				// + "\tvalue:   " + value);

				readNextKey();
			} catch (IOException ie) {
				throw new RuntimeException("problem advancing post rec#" + ctr,
						ie);
			}
			reporter.progress();
			return value;
		}

		public void appendResKV(KEY key, OUTVALUE val) throws IOException {
			preserveWriter.appendResKV(key, val);
			// LOG.info("appendResKV: key:   "+key + "\tvalue:   " + val);
		}

		public void remove() {
			throw new RuntimeException("not implemented");
		}

		// / Auxiliary methods

		/** Start processing next unique key. */
		void nextKey() throws IOException {
			// read until we find a new key
			while (hasNext) {
				readNextKey();
			}
			++ctr;

			// move the next key to the current one
			KEY tmpKey = key;
			key = nextKey;
			nextKey = tmpKey;
			hasNext = more;
		}

		/** True iff more keys remain. */
		boolean more() {
			return more;
		}

		/** The current key. */
		KEY getKey() {
			return key;
		}

		/**
		 * read the next key
		 */
		private void readNextKey() throws IOException {
			more = in.next();
			if (more) {
				DataInputBuffer nextKeyBytes = in.getKey();
				keyIn.reset(nextKeyBytes.getData(), nextKeyBytes.getPosition(),
						nextKeyBytes.getLength());
				nextKey = keyDeserializer.deserialize(nextKey);
				hasNext = key != null
						&& (comparator.compare(key, nextKey) == 0);
			} else {
				hasNext = false;
			}
		}

		/**
		 * Read the next value
		 * 
		 * @throws IOException
		 */
		private void readNextValue() throws IOException {
			DataInputBuffer nextValueBytes = in.getValue();
			valueIn.reset(nextValueBytes.getData(),
					nextValueBytes.getPosition(), nextValueBytes.getLength());
			value = valDeserializer.deserialize(value);
		}

		private void readNextSource() throws IOException {
			DataInputBuffer nextSourceBytes = in.getSKey();
			skeyIn.reset(nextSourceBytes.getData(),
					nextSourceBytes.getPosition(), nextSourceBytes.getLength());
			skey = skeyDeserializer.deserialize(skey); // maybe not useful
		}

		public void close() throws IOException {
			preserveWriter.close();
		}
	}

	/**
	 * for extracting the useful k [v] for reduce function, reading from the
	 * delta file and the preserve file
	 * 
	 * @author yzhang
	 * 
	 * @param <KEY>
	 * @param <VALUE>
	 * @param <SOURCEKEY>
	 */
	static class ValuesInFileIterator<KEY, VALUE, SOURCEKEY, OUTVALUE>
			implements
			IncrementalSourceValuesIterator<KEY, VALUE, SOURCEKEY, OUTVALUE> {

		class Record {
			KEY key;
			SOURCEKEY source;
			VALUE value;
			OUTVALUE outvalue;

			boolean isIntermediate = true;
			boolean hasNext = false;
			boolean processed = false;

			Record(KEY key, SOURCEKEY source, VALUE value, OUTVALUE outvalue) {
				this.key = key;
				this.source = source;
				this.value = value;
				this.outvalue = outvalue;
			}

			@Override
			public String toString() {
				return key + "\t" + source + "\t" + value + "\t" + outvalue;
			}
		}

		/**
		 * 3LineReader, a 3 bucket buffer that buffers 3 lines of the delta
		 * file, it handles the case that two records with consecutive '-' and
		 * new value, which needs to update it with the later one
		 */
		class ThreeLineBuffer {
			public Record lineA;

			public Record lineB;

			public Record lineC;

			private VALUE nv;

			public ThreeLineBuffer(VALUE negativeV) {
				nv = negativeV;
			}

			public boolean isEmpty() {
				return lineA == null && lineB == null && lineC == null;
			}

			public boolean isFull() {
				return lineA != null && lineB != null && lineC != null;
			}

			public List<Record> getValue() {

				// LOG.info("* A:" + lineA + " negativeV " + nv);
				// LOG.info("* B:" + lineB);
				// LOG.info("* C:" + lineC);

				List<Record> recordsList = new ArrayList<Record>();

				// A && B row same return 1 tuple combining A and B
				if (lineA.key.equals(lineB.key)
						&& lineA.source.equals(lineB.source)) {

					VALUE deltaValue = null;

					if (!lineA.value.equals(nv)) {
						deltaValue = lineA.value;
					} else if (!lineB.value.equals(nv)) {
						deltaValue = lineB.value;
					} else {
						deltaValue = negativeV;
					}

					Record record = new Record(lineA.key, lineA.source,
							deltaValue, null);

					// LOG.info("insert " + record);

					lineA = lineC;
					lineB = null;
					lineC = null;

					recordsList.add(record);
				}
				// B && C row same, return 2 tuples, A and B&C
				else if (lineB.key.equals(lineC.key)
						&& lineB.source.equals(lineC.source)) {

					Record record1 = new Record(lineA.key, lineA.source,
							lineA.value, null);

					// LOG.info("insert " + record1);

					recordsList.add(record1);

					VALUE deltaValue = null;

					if (!lineB.value.equals(nv)) {
						deltaValue = lineB.value;
					} else if (!lineC.value.equals(nv)) {
						deltaValue = lineC.value;
					} else {
						deltaValue = negativeV;
					}

					Record record2 = new Record(lineB.key, lineB.source,
							deltaValue, null);

					// LOG.info("insert " + record2);

					lineA = null;
					lineB = null;
					lineC = null;

					recordsList.add(record2);
				}
				// A && B row not same, return 2 tuples A and B
				else if (!lineA.key.equals(lineB.key)
						|| !lineA.source.equals(lineB.source)) {

					Record record1 = new Record(lineA.key, lineA.source,
							lineA.value, null);

					// LOG.info("insert " + record1);

					recordsList.add(record1);

					Record record2 = new Record(lineB.key, lineB.source,
							lineB.value, null);

					// LOG.info("insert " + record2);

					recordsList.add(record2);

					lineA = lineC;
					lineB = null;
					lineC = null;
				}

				return recordsList;

			}

			public List<Record> getRestValue() {

				// System.out.println("*1 A:" + Arrays.toString(lineA));
				// System.out.println("*2 B:" + Arrays.toString(lineB));
				// System.out.println("*3 C:" + Arrays.toString(lineC));

				List<Record> recordsList = new ArrayList<Record>();

				// only one record left in the buffer
				if (lineA != null && lineB == null && lineC == null) {
					Record record = new Record(lineA.key, lineA.source,
							lineA.value, null);

					recordsList.add(record);
				}
				// two records left in the buffer
				else if (lineA != null && lineB != null) {
					// the two are with the same key and source, one of them is
					// useful
					if (lineA.key.equals(lineB.key)
							&& lineA.source.equals(lineB.source)) {
						VALUE deltaValue = null;

						if (!lineA.value.equals(negativeV)) {
							deltaValue = lineA.value;
						} else if (!lineB.value.equals(negativeV)) {
							deltaValue = lineB.value;
						} else {
							deltaValue = negativeV;
						}

						Record record = new Record(lineA.key, lineA.source,
								deltaValue, null);

						// lineA = lineC;
						lineA = null;
						lineB = null;
						lineC = null;

						recordsList.add(record);
					}
					// the two not same, return both
					else {
						Record record1 = new Record(lineA.key, lineA.source,
								lineA.value, null);

						recordsList.add(record1);

						Record record2 = new Record(lineB.key, lineB.source,
								lineB.value, null);

						recordsList.add(record2);
					}
				}

				lineA = null;
				lineB = null;
				lineC = null;

				return recordsList;

			}
		}

		/**
		 * the class for extracting the useful kv pair from the delta file
		 * 
		 * @author yzhang
		 * 
		 */
		class DeltaFileReader {

			ThreeLineBuffer threeLineBuffer;
			RawKeyValueSourceIterator in;
			Deserializer<KEY> keyDeserializer;
			Deserializer<VALUE> valDeserializer;
			Deserializer<SOURCEKEY> skeyDeserializer;
			DataInputBuffer keyIn = new DataInputBuffer();
			DataInputBuffer valueIn = new DataInputBuffer();
			DataInputBuffer skeyIn = new DataInputBuffer();

			LinkedList<Record> recordQueue = new LinkedList<Record>();

			public DeltaFileReader(RawKeyValueSourceIterator in, JobConf job,
					Class<KEY> keyClass, Class<VALUE> valClass,
					Class<SOURCEKEY> skeyClass, VALUE negV) throws IOException {
				this.in = in;
				SerializationFactory serializationFactory = new SerializationFactory(
						job);
				this.keyDeserializer = serializationFactory
						.getDeserializer(keyClass);
				this.keyDeserializer.open(keyIn);
				this.valDeserializer = serializationFactory
						.getDeserializer(valClass);
				this.valDeserializer.open(this.valueIn);
				this.skeyDeserializer = serializationFactory
						.getDeserializer(skeyClass);
				this.skeyDeserializer.open(this.skeyIn);

				threeLineBuffer = new ThreeLineBuffer(negV);
				extractRecords();
			}

			public Record nextRecord() throws IOException {
				// LOG.info("queue size is " + recordQueue.size());
				if (recordQueue.isEmpty()) {
					// LOG.info("extract records");
					extractRecords();
				}

				return recordQueue.poll();
			}

			private void extractRecords() throws IOException {
				while (in.next()) {

					if (threeLineBuffer.lineA == null) {
						KEY key = readKey();
						SOURCEKEY source = readSource();
						VALUE value = readValue();

						// LOG.info("delta file read " + key + "\t" + source +
						// "\t" + value);

						threeLineBuffer.lineA = new Record(key, source, value,
								null);
						continue;
					}

					if (threeLineBuffer.lineB == null) {
						KEY key = readKey();
						SOURCEKEY source = readSource();
						VALUE value = readValue();

						// LOG.info("delta file read " + key + "\t" + source +
						// "\t" + value);

						threeLineBuffer.lineB = new Record(key, source, value,
								null);

						continue;
					}

					KEY key = readKey();
					SOURCEKEY source = readSource();
					VALUE value = readValue();

					// LOG.info("delta file read " + key + "\t" + source + "\t"
					// + value);

					threeLineBuffer.lineC = new Record(key, source, value, null);

					List<Record> newrecordsList = threeLineBuffer.getValue();

					recordQueue.addAll(newrecordsList);
					return;
				}

				if (!threeLineBuffer.isEmpty()) {
					List<Record> newrecordsList = threeLineBuffer
							.getRestValue();
					recordQueue.addAll(newrecordsList);
				}
			}

			public KEY readKey() throws IOException {
				KEY key = null;
				DataInputBuffer keyBytes = in.getKey();
				keyIn.reset(keyBytes.getData(), keyBytes.getPosition(),
						keyBytes.getLength());
				key = keyDeserializer.deserialize(key);

				return key;
			}

			public SOURCEKEY readSource() throws IOException {
				SOURCEKEY source = null;
				DataInputBuffer sourceBytes = in.getSKey();
				skeyIn.reset(sourceBytes.getData(), sourceBytes.getPosition(),
						sourceBytes.getLength());
				source = skeyDeserializer.deserialize(source);

				return source;
			}

			public VALUE readValue() throws IOException {
				VALUE value = null;
				DataInputBuffer valueBytes = in.getValue();
				valueIn.reset(valueBytes.getData(), valueBytes.getPosition(),
						valueBytes.getLength());
				value = valDeserializer.deserialize(value);

				return value;
			}
		}

		/**
		 * the class for extracting the kv pair from the preserve file based on
		 * the given kv from delta file
		 * 
		 * @author yzhang
		 * 
		 */
		class WrapPreserveFile {

			IFile.PreserveFile<KEY, VALUE, SOURCEKEY, OUTVALUE> preserveFile;
			Deserializer<KEY> keyDeserializer1;
			Deserializer<VALUE> valDeserializer1;
			Deserializer<SOURCEKEY> skeyDeserializer1;
			Deserializer<OUTVALUE> outvalDeserializer1;
			DataInputBuffer keyIn1 = new DataInputBuffer();
			DataInputBuffer valueIn1 = new DataInputBuffer();
			DataInputBuffer skeyIn1 = new DataInputBuffer();
			DataInputBuffer outvalueIn1 = new DataInputBuffer();
			RawComparator<SOURCEKEY> comparator;
			boolean hasNext = true;
			boolean fileEnd = false;
			Record preserveRecord;
			int keyhash;

			public WrapPreserveFile(
					JobConf job,
					IFile.PreserveFile<KEY, VALUE, SOURCEKEY, OUTVALUE> preserveFile,
					Class<KEY> keyClass, Class<VALUE> valClass,
					Class<SOURCEKEY> skeyClass, Class<OUTVALUE> outvalClass,
					RawComparator<SOURCEKEY> com) throws IOException {
				this.preserveFile = preserveFile;
				comparator = com;
				SerializationFactory serializationFactory = new SerializationFactory(
						job);
				this.keyDeserializer1 = serializationFactory
						.getDeserializer(keyClass);
				this.keyDeserializer1.open(keyIn1);
				this.valDeserializer1 = serializationFactory
						.getDeserializer(valClass);
				this.valDeserializer1.open(valueIn1);
				this.skeyDeserializer1 = serializationFactory
						.getDeserializer(skeyClass);
				this.skeyDeserializer1.open(skeyIn1);
				this.outvalDeserializer1 = serializationFactory
						.getDeserializer(outvalClass);
				this.outvalDeserializer1.open(outvalueIn1);
			}

			public void close() throws IOException {
				preserveFile.close();
			}

			// return hashcode, conflict-avoidence hashcode
			public boolean seek(KEY k, int rehash, IntWritable hashcode)
					throws IOException {
				return preserveFile.seekKey(k, rehash, hashcode);
			}

			public Record getNextRecord(KEY k) throws IOException {
				KEY key1 = null;
				SOURCEKEY source1 = null;
				VALUE value1 = null;
				OUTVALUE outvalue1 = null;

				IFile.PreserveFile.TYPE returnType = preserveFile.next(keyIn1,
						valueIn1, skeyIn1, outvalueIn1);
				if (returnType == IFile.PreserveFile.TYPE.MORE) {
					// this might be a problem for deserializing
					// keyDeserializer1.open(keyIn1);
					key1 = keyDeserializer1.deserialize(key1);
					// valDeserializer1.open(valueIn1);
					value1 = valDeserializer1.deserialize(value1);
					// skeyDeserializer1.open(skeyIn1);
					source1 = skeyDeserializer1.deserialize(source1);

					LOG.info("get record: " + key1 + "\t" + value1 + "\t"
							+ source1);

					if (key1.equals(k)) {
						preserveRecord = new Record(key1, source1, value1, null);
						// LOG.info("read preserve record: " + preserveRecord);
						return preserveRecord;
					} else {
						return null;
					}
				} else if (returnType == IFile.PreserveFile.TYPE.RECORDEND) {
					// read the preserved output key value pair
					key1 = keyDeserializer1.deserialize(key1);
					outvalue1 = outvalDeserializer1.deserialize(outvalue1);

					LOG.info("get record end: " + key1 + "\t" + outvalue1);

					if (key1.equals(k)) {
						preserveRecord = new Record(key1, null, null, outvalue1);
						preserveRecord.isIntermediate = false;
						// LOG.info("read preserve record: " + preserveRecord);
						return preserveRecord;
					} else {
						return null;
					}
				} else {
					// read the end of the preservefile
					return null;
				}
			}

			public void update(int keyhash, KEY t1, VALUE t2, SOURCEKEY t3)
					throws IOException {
				preserveFile.updateShuffleKVS(keyhash, t1, t2, t3);
			}

			public void update(int keyhash, KEY t1, OUTVALUE t4)
					throws IOException {
				preserveFile.updateResKV(keyhash, t1, t4);
			}
		}

		private boolean more = true;
		protected RawKeyValueSourceIterator in;
		private VALUE negativeV;
		private DeltaFileReader deltaReader;
		private WrapPreserveFile wrapPreserveFile;
		private RawComparator<SOURCEKEY> comparator2;
		private Record currDeltaRecord;
		private Record nextDeltaRecord;
		private Record currPreserveRecord;
		private Record returnTuple;
		protected Progressable reporter;
		private JobConf job;
		private boolean preserveHasNext = true;
		private IntWritable keyHashBuffer = new IntWritable();
		private OUTVALUE preservedOutValue = null;

		public ValuesInFileIterator(
				RawKeyValueSourceIterator in,
				int iteration,
				RawComparator<KEY> comparator,
				Class<KEY> keyClass,
				Class<VALUE> valClass,
				Class<SOURCEKEY> skeyClass,
				Class<OUTVALUE> outvalueClass,
				IFile.PreserveFile<KEY, VALUE, SOURCEKEY, OUTVALUE> preserveWriter,
				int taskid, VALUE negativeV, Configuration conf,
				Progressable reporter) throws IOException {
			this.negativeV = negativeV;
			this.in = in;
			this.reporter = reporter;
			job = (JobConf) conf;

			comparator2 = (RawComparator<SOURCEKEY>) WritableComparator.get(job
					.getStaticKeyClass().asSubclass(WritableComparable.class));

			this.deltaReader = new DeltaFileReader(in, job, keyClass, valClass,
					skeyClass, negativeV);
			wrapPreserveFile = new WrapPreserveFile(job, preserveWriter,
					keyClass, valClass, skeyClass, outvalueClass, comparator2);

			currDeltaRecord = deltaReader.nextRecord();
			if (currDeltaRecord == null) {
				LOG.info("no entries in delta file!!!");
			} else {
				nextDeltaRecord = deltaReader.nextRecord();

				LOG.info("delta extract: " + currDeltaRecord);
				// LOG.info("delta extract: " + nextDeltaRecord);

				int trial = 0;
				while (currPreserveRecord == null
						&& wrapPreserveFile.seek(currDeltaRecord.key, trial,
								keyHashBuffer)) {
					trial++;
					currPreserveRecord = wrapPreserveFile
							.getNextRecord(currDeltaRecord.key);
					LOG.info("record: " + currPreserveRecord);
				}

				if (trial == 0)
					throw new RuntimeException("no entries in preserve file!!!");
				// LOG.info("initial phase:  to match " + currDeltaRecord.key +
				// " is " + currPreserveRecord);
			}
		}

		// / Iterator methods
		public boolean hasNext() {
			if (currDeltaRecord == null) {
				return preserveHasNext;
			} else if (currPreserveRecord == null) {
				// if currdeltarecord is still that one, continue process the
				// currdeltarecord
				return currDeltaRecord.hasNext;
				// if(nextDeltaRecord != null) return
				// currDeltaRecord.key.equals(nextDeltaRecord.key);
			} else {
				// LOG.info("check has next " + currDeltaRecord.key + "\t" +
				// currPreserveRecord.key);
				return currDeltaRecord.key.equals(currPreserveRecord.key);
			}
		}

		private int ctr = 0;

		/**
		 * Shimin:read all the reduce input kvs with the same key together, and
		 * return kv one-by-one Yanfeng: but has memory-overflow risk Let's try
		 * it first and compare their performance, add a new class to handle
		 * this, see IncrementalBufferValuesIterator
		 */

		/**
		 * zhangyf has changed the programming model of the next method. In this
		 * next() method, it might return a negative value users set, then the
		 * user's program should take care of this negative value and skip it.
		 */
		public VALUE next() {
			try {
				if (currDeltaRecord != null)
					currDeltaRecord.hasNext = false;
				preserveHasNext = false;

				if (currDeltaRecord == null) {
					// process the preserve file entry
					returnTuple = new Record(currPreserveRecord.key,
							currPreserveRecord.source,
							currPreserveRecord.value, null);

					currPreserveRecord = wrapPreserveFile
							.getNextRecord(currPreserveRecord.key);

					if (currPreserveRecord.isIntermediate) {
						preserveHasNext = true;
					} else {
						preserveHasNext = false;
						preservedOutValue = currPreserveRecord.outvalue;
					}
					/*
					 * if(currPreserveRecord != null){ preserveHasNext = true;
					 * }else{ preserveHasNext = false; }
					 */

					wrapPreserveFile.update(keyHashBuffer.get(),
							returnTuple.key, returnTuple.value,
							returnTuple.source);
					// LOG.info("deltanull write: " + returnTuple.key + "\t" +
					// returnTuple.value + "\t" + returnTuple.source + "\t" +
					// currPreserveRecord);

				} else if (currPreserveRecord == null) {
					// process the new file entry
					returnTuple = new Record(currDeltaRecord.key,
							currDeltaRecord.source, currDeltaRecord.value, null);

					if (more) {
						// read a line of delta file if these two are with the
						// same key
						if (currDeltaRecord.key.equals(nextDeltaRecord.key)) {
							currDeltaRecord = nextDeltaRecord;
							nextDeltaRecord = deltaReader.nextRecord();
							if (nextDeltaRecord == null)
								more = false;

							currDeltaRecord.hasNext = true;
							currDeltaRecord.processed = false;
							// LOG.info("delta extract: " + nextDeltaRecord);
						}
					} else {
						currDeltaRecord = null;
					}

					wrapPreserveFile.update(keyHashBuffer.get(),
							returnTuple.key, returnTuple.value,
							returnTuple.source);

					// LOG.info("extract delta key " + currDeltaRecord.key +
					// "\t" + currDeltaRecord.source + "\t" +
					// currDeltaRecord.value);
				} else if (currDeltaRecord.key.equals(currPreserveRecord.key)) {
					int cmpres = comparator2.compare(currDeltaRecord.source,
							currPreserveRecord.source);

					// LOG.info("comparing " + currDeltaRecord.source + " and "
					// + currPreserveRecord.source + " result is " + cmpres);

					if (cmpres < 0) {
						if (nextDeltaRecord == null) {
							returnTuple = new Record(currDeltaRecord.key,
									currDeltaRecord.source,
									currDeltaRecord.value, null);
							preserveHasNext = true;
							currDeltaRecord = null;
						} else {
							// read the delta file if we have more new delta kv
							// pairs for the same key
							if (currDeltaRecord.key.equals(nextDeltaRecord.key)) {
								// process the new file entry
								returnTuple = new Record(currDeltaRecord.key,
										currDeltaRecord.source,
										currDeltaRecord.value, null);

								currDeltaRecord = nextDeltaRecord;
								currDeltaRecord.processed = false;
								nextDeltaRecord = deltaReader.nextRecord();

								if (nextDeltaRecord == null) {
									more = false;
								} else if (currDeltaRecord.key
										.equals(nextDeltaRecord.key)) {
									currDeltaRecord.hasNext = true;
								}

								// LOG.info("delta extract: " +
								// nextDeltaRecord);

								// LOG.info("process new delta: " +
								// returnTuple.key + "\t" + returnTuple.source +
								// "\t" + returnTuple.value);
							}
							// not processed delta record
							else if (!currDeltaRecord.processed) {
								returnTuple = new Record(currDeltaRecord.key,
										currDeltaRecord.source,
										currDeltaRecord.value, null);
								currDeltaRecord.processed = true;
							}
							// already processed delta record, then read the
							// preserve file for processing the remaining kv
							// pairs for the key
							else {
								// process the remaining preserve file entry
								returnTuple = new Record(
										currPreserveRecord.key,
										currPreserveRecord.source,
										currPreserveRecord.value, null);

								currPreserveRecord = wrapPreserveFile
										.getNextRecord(currDeltaRecord.key);

								if (!currPreserveRecord.isIntermediate) {
									preservedOutValue = currPreserveRecord.outvalue;
									currPreserveRecord = null;
								}
								// LOG.info("process preserve data: " +
								// returnTuple.key + "\t" + returnTuple.source +
								// "\t" + returnTuple.value);
							}
						}

						wrapPreserveFile.update(keyHashBuffer.get(),
								returnTuple.key, returnTuple.value,
								returnTuple.source);
						// LOG.info("preserve write1: " + returnTuple.key + "\t"
						// + returnTuple.value + "\t" + returnTuple.source);
					} else if (cmpres > 0) {
						// process the preserve file entry
						returnTuple = new Record(currPreserveRecord.key,
								currPreserveRecord.source,
								currPreserveRecord.value, null);

						if (currDeltaRecord.key.equals(currPreserveRecord.key)) {
							currDeltaRecord.hasNext = true;
						}

						currPreserveRecord = wrapPreserveFile
								.getNextRecord(currDeltaRecord.key);

						if (!currPreserveRecord.isIntermediate) {
							preservedOutValue = currPreserveRecord.outvalue;
							currPreserveRecord = null;
						}

						wrapPreserveFile.update(keyHashBuffer.get(),
								returnTuple.key, returnTuple.value,
								returnTuple.source);
						// LOG.info("preserve write2: " + returnTuple.key + "\t"
						// + returnTuple.value + "\t" + returnTuple.source +
						// "\t" + currPreserveRecord);
						// LOG.info("process preserve data: " + returnTuple.key
						// + "\t" + returnTuple.source + "\t" +
						// returnTuple.value);
					} else {
						// read a line of preserve file, meaning skipping that
						// record
						currPreserveRecord = wrapPreserveFile
								.getNextRecord(currDeltaRecord.key);

						if (!currPreserveRecord.isIntermediate) {
							preservedOutValue = currPreserveRecord.outvalue;
							currPreserveRecord = null;
						}

						returnTuple = new Record(currDeltaRecord.key,
								currDeltaRecord.source, currDeltaRecord.value,
								null);

						// if equal, replace the preserver file entry otherwise
						// skip it
						if (!currDeltaRecord.value.equals(negativeV)) {
							wrapPreserveFile.update(keyHashBuffer.get(),
									returnTuple.key, returnTuple.value,
									returnTuple.source);
							// LOG.info("replace record: " + returnTuple.key +
							// "\t" + returnTuple.source + "\t" +
							// returnTuple.value);
							// LOG.info("preserve write3: " + returnTuple.key +
							// "\t" + returnTuple.value + "\t" +
							// returnTuple.source);
						} else {
							// LOG.info("skip record: " + returnTuple.key + "\t"
							// + returnTuple.source);
						}

						// read a line of delta file if these two are with the
						// same key
						if (nextDeltaRecord == null) {
							if (currPreserveRecord != null) {
								preserveHasNext = true;
							} else {
								preserveHasNext = false;
							}

							currDeltaRecord = null;
							more = false;
						} else {
							if (currDeltaRecord.key.equals(nextDeltaRecord.key)) {
								currDeltaRecord = nextDeltaRecord;
								currDeltaRecord.processed = false;
								nextDeltaRecord = deltaReader.nextRecord();
								// LOG.info("delta extract: " +
								// nextDeltaRecord);

								currDeltaRecord.hasNext = true;

								if (nextDeltaRecord == null) {
									more = false;
								}
							} else {
								currDeltaRecord.processed = true;
							}
						}
					}
				} else {
					throw new IOException("will this happer?");
					/*
					 * //process the new file entry returnTuple = new
					 * Record(currDeltaRecord.key, currDeltaRecord.source,
					 * currDeltaRecord.value);
					 * 
					 * //read a line of delta file if these two are with the
					 * same key
					 * if(currDeltaRecord.key.equals(nextDeltaRecord.key)){
					 * currDeltaRecord = nextDeltaRecord;
					 * currDeltaRecord.processed = false; nextDeltaRecord =
					 * deltaReader.nextRecord(); if(nextDeltaRecord == null)
					 * more = false;
					 * 
					 * currDeltaRecord.hasNext = true;
					 * //LOG.info("delta extract: " + nextDeltaRecord); }
					 * 
					 * wrapPreserveFile.update(keyHashBuffer.get(), false,
					 * returnTuple.key, returnTuple.value, returnTuple.source);
					 * //LOG.info("process new delta: " + returnTuple.key + "\t"
					 * + returnTuple.source + "\t" + returnTuple.value);
					 * //LOG.info("preserve write4: " + returnTuple.key + "\t" +
					 * returnTuple.value + "\t" + returnTuple.source);
					 */
				}
			} catch (IOException ie) {
				throw new RuntimeException("problem advancing post rec#" + ctr,
						ie);
			}

			reporter.progress();
			return returnTuple.value;
		}

		public void remove() {
			throw new RuntimeException("not implemented");
		}

		public OUTVALUE getPreservedOutValue() {
			return preservedOutValue;
		}

		public void updateResKV(KEY key, OUTVALUE outvalue) throws IOException {
			wrapPreserveFile.update(keyHashBuffer.get(), key, outvalue);
		}

		/** Start processing next unique key. */
		public void nextKey() throws IOException {
			/*
			 * if(!more){ preserveReader.setNextKey(); currPreserveRecord =
			 * preserveReader.getNextValue(currDeltaRecord.key);
			 * currDeltaRecord.hasNext = true; ++ctr; LOG.info("no more data");
			 * return; }
			 */
			if (currDeltaRecord == null) {
				// LOG.info("no more data");
				return;
			}

			currDeltaRecord = nextDeltaRecord;
			currDeltaRecord.processed = false;
			nextDeltaRecord = deltaReader.nextRecord();

			// LOG.info("next record: " + currDeltaRecord);

			int trial = 0;
			currPreserveRecord = null;
			preservedOutValue = null;
			while (currPreserveRecord == null
					&& wrapPreserveFile.seek(currDeltaRecord.key, trial,
							keyHashBuffer)) {
				trial++;
				currPreserveRecord = wrapPreserveFile
						.getNextRecord(currDeltaRecord.key);
			}

			currDeltaRecord.hasNext = true;
			++ctr;

			if (nextDeltaRecord == null) {
				more = false;
			}
		}

		/** True iff more keys remain. */
		public boolean more() {
			// return more;
			return currDeltaRecord != null;
		}

		/** The current key. */
		public KEY getKey() {
			return currDeltaRecord.key;
		}

		public void close() throws IOException {
			wrapPreserveFile.close();
		}
	}

	static class ValuesInMemIterator<KEY, VALUE, SOURCEKEY, OUTVALUE>
			implements
			IncrementalSourceValuesIterator<KEY, VALUE, SOURCEKEY, OUTVALUE> {

		class Record {
			KEY key;
			SOURCEKEY source;
			VALUE value;
			OUTVALUE outvalue;

			boolean isIntermediate = true;
			boolean hasNext = false;
			boolean processed = false;

			Record(KEY key, SOURCEKEY source, VALUE value, OUTVALUE outvalue) {
				this.key = key;
				this.source = source;
				this.value = value;
				this.outvalue = outvalue;
			}

			@Override
			public String toString() {
				return key + "\t" + source + "\t" + value + "\t" + outvalue;
			}
		}

		class IKValues {
			KEY iKey;
			LinkedList<VALUE> ivalues;
			OUTVALUE ovalue;

			IKValues(KEY iKey, LinkedList<VALUE> ivalues, OUTVALUE ovalue) {
				this.iKey = iKey;
				this.ivalues = ivalues;
				this.ovalue = ovalue;
			}
		}

		/**
		 * 3LineReader, a 3 bucket buffer that buffers 3 lines of the delta file
		 */
		class ThreeLineBuffer {
			public Record lineA;

			public Record lineB;

			public Record lineC;

			private VALUE nv;

			public ThreeLineBuffer(VALUE negativeV) {
				nv = negativeV;
			}

			public boolean isEmpty() {
				return lineA == null && lineB == null && lineC == null;
			}

			public boolean isFull() {
				return lineA != null && lineB != null && lineC != null;
			}

			public List<Record> getValue() {

				// LOG.info("* A:" + lineA + " negativeV " + nv);
				// LOG.info("* B:" + lineB);
				// LOG.info("* C:" + lineC);

				List<Record> recordsList = new ArrayList<Record>();

				// A && B row same return 1 tuple combining A and B
				if (lineA.key.equals(lineB.key)
						&& lineA.source.equals(lineB.source)) {

					VALUE deltaValue = null;

					if (!lineA.value.equals(nv)) {
						deltaValue = lineA.value;
					} else if (!lineB.value.equals(nv)) {
						deltaValue = lineB.value;
					} else {
						deltaValue = negativeV;
					}

					Record record = new Record(lineA.key, lineA.source,
							deltaValue, null);

					// LOG.info("insert " + record);

					lineA = lineC;
					lineB = null;
					lineC = null;

					recordsList.add(record);
				}
				// B && C row same, return 2 tuples, A and B&C
				else if (lineB.key.equals(lineC.key)
						&& lineB.source.equals(lineC.source)) {

					Record record1 = new Record(lineA.key, lineA.source,
							lineA.value, null);

					// LOG.info("insert " + record1);

					recordsList.add(record1);

					VALUE deltaValue = null;

					if (!lineB.value.equals(nv)) {
						deltaValue = lineB.value;
					} else if (!lineC.value.equals(nv)) {
						deltaValue = lineC.value;
					} else {
						deltaValue = negativeV;
					}

					Record record2 = new Record(lineB.key, lineB.source,
							deltaValue, null);

					// LOG.info("insert " + record2);

					lineA = null;
					lineB = null;
					lineC = null;

					recordsList.add(record2);
				}
				// A && B row not same, return 2 tuples A and B
				else if (!lineA.key.equals(lineB.key)
						|| !lineA.source.equals(lineB.source)) {

					Record record1 = new Record(lineA.key, lineA.source,
							lineA.value, null);

					// LOG.info("insert " + record1);

					recordsList.add(record1);

					Record record2 = new Record(lineB.key, lineB.source,
							lineB.value, null);

					// LOG.info("insert " + record2);

					recordsList.add(record2);

					lineA = lineC;
					lineB = null;
					lineC = null;
				}

				return recordsList;

			}

			public List<Record> getRestValue() {

				// System.out.println("*1 A:" + Arrays.toString(lineA));
				// System.out.println("*2 B:" + Arrays.toString(lineB));
				// System.out.println("*3 C:" + Arrays.toString(lineC));

				List<Record> recordsList = new ArrayList<Record>();

				// only one record left in the buffer
				if (lineA != null && lineB == null && lineC == null) {
					Record record = new Record(lineA.key, lineA.source,
							lineA.value, null);

					recordsList.add(record);
				}
				// two records left in the buffer
				else if (lineA != null && lineB != null) {
					// the two are with the same key and source, one of them is
					// useful
					if (lineA.key.equals(lineB.key)
							&& lineA.source.equals(lineB.source)) {
						VALUE deltaValue = null;

						if (!lineA.value.equals(negativeV)) {
							deltaValue = lineA.value;
						} else if (!lineB.value.equals(negativeV)) {
							deltaValue = lineB.value;
						} else {
							deltaValue = negativeV;
						}

						Record record = new Record(lineA.key, lineA.source,
								deltaValue, null);

						// lineA = lineC;
						lineA = null;
						lineB = null;
						lineC = null;

						recordsList.add(record);
					}
					// the two not same, return both
					else {
						Record record1 = new Record(lineA.key, lineA.source,
								lineA.value, null);

						recordsList.add(record1);

						Record record2 = new Record(lineB.key, lineB.source,
								lineB.value, null);

						recordsList.add(record2);
					}
				}

				lineA = null;
				lineB = null;
				lineC = null;

				return recordsList;

			}
		}

		/**
		 * the class for extracting the useful kv pair from the delta file
		 * 
		 * @author yzhang
		 * 
		 */
		class DeltaFileReader {

			ThreeLineBuffer threeLineBuffer;
			RawKeyValueSourceIterator in;
			Deserializer<KEY> keyDeserializer;
			Deserializer<VALUE> valDeserializer;
			Deserializer<SOURCEKEY> skeyDeserializer;
			DataInputBuffer keyIn = new DataInputBuffer();
			DataInputBuffer valueIn = new DataInputBuffer();
			DataInputBuffer skeyIn = new DataInputBuffer();

			Record currRec = null;

			LinkedList<Record> recordQueue = new LinkedList<Record>();

			public DeltaFileReader(RawKeyValueSourceIterator in, JobConf job,
					Class<KEY> keyClass, Class<VALUE> valClass,
					Class<SOURCEKEY> skeyClass, VALUE negV) throws IOException {
				this.in = in;
				SerializationFactory serializationFactory = new SerializationFactory(
						job);
				this.keyDeserializer = serializationFactory
						.getDeserializer(keyClass);
				this.keyDeserializer.open(keyIn);
				this.valDeserializer = serializationFactory
						.getDeserializer(valClass);
				this.valDeserializer.open(this.valueIn);
				this.skeyDeserializer = serializationFactory
						.getDeserializer(skeyClass);
				this.skeyDeserializer.open(this.skeyIn);

				threeLineBuffer = new ThreeLineBuffer(negV);
				extractRecords();
			}

			// get the values for the same key
			public LinkedList<Record> getRecords() throws IOException {
				// LOG.info("get records: ");

				LinkedList<Record> values = new LinkedList<Record>();

				if (recordQueue.isEmpty()) {
					// LOG.info("extract records");
					extractRecords();
				}

				currRec = recordQueue.poll();

				if (currRec == null) {
					return null;
				} else {
					values.push(currRec);
					// LOG.info("push value " + currRec);
				}

				if (recordQueue.isEmpty()) {
					// LOG.info("extract records");
					extractRecords();
				}

				Record nextRec = recordQueue.peek();
				// LOG.info("currRec " + currRec + " nextRec " + nextRec);
				if (nextRec == null)
					return values;

				while (currRec.key.equals(nextRec.key)) {
					nextRec = recordQueue.poll();
					values.push(nextRec);

					// LOG.info("push value " + nextRec);

					if (recordQueue.isEmpty()) {
						// LOG.info("extract records");
						extractRecords();
					}

					nextRec = recordQueue.peek();
					if (nextRec == null)
						return values;
					// LOG.info("currRec " + currRec + " nextRec " + nextRec);
				}

				return values;
			}

			
			
			public Record nextRecord() throws IOException {
				// LOG.info("queue size is " + recordQueue.size());
				if (recordQueue.isEmpty()) {
					// LOG.info("extract records");
					extractRecords();
				}

				return recordQueue.poll();
			}

			private void extractRecords() throws IOException {
				while (in.next()) {

					if (threeLineBuffer.lineA == null) {
						KEY key = readKey();
						SOURCEKEY source = readSource();
						VALUE value = readValue();

						// LOG.info("delta file read " + key + "\t" + source +
						// "\t" + value);

						threeLineBuffer.lineA = new Record(key, source, value,
								null);
						continue;
					}

					if (threeLineBuffer.lineB == null) {
						KEY key = readKey();
						SOURCEKEY source = readSource();
						VALUE value = readValue();

						// LOG.info("delta file read " + key + "\t" + source +
						// "\t" + value);

						threeLineBuffer.lineB = new Record(key, source, value,
								null);

						continue;
					}

					KEY key = readKey();
					SOURCEKEY source = readSource();
					VALUE value = readValue();

					// LOG.info("delta file read " + key + "\t" + source + "\t"
					// + value);

					threeLineBuffer.lineC = new Record(key, source, value, null);

					List<Record> newrecordsList = threeLineBuffer.getValue();

					recordQueue.addAll(newrecordsList);
					return;
				}

				if (!threeLineBuffer.isEmpty()) {
					List<Record> newrecordsList = threeLineBuffer
							.getRestValue();
					recordQueue.addAll(newrecordsList);
				}
			}

			public KEY readKey() throws IOException {
				KEY key = null;
				DataInputBuffer keyBytes = in.getKey();
				keyIn.reset(keyBytes.getData(), keyBytes.getPosition(),
						keyBytes.getLength());
				key = keyDeserializer.deserialize(key);

				return key;
			}

			public SOURCEKEY readSource() throws IOException {
				SOURCEKEY source = null;
				DataInputBuffer sourceBytes = in.getSKey();
				skeyIn.reset(sourceBytes.getData(), sourceBytes.getPosition(),
						sourceBytes.getLength());
				source = skeyDeserializer.deserialize(source);

				return source;
			}

			public VALUE readValue() throws IOException {
				VALUE value = null;
				DataInputBuffer valueBytes = in.getValue();
				valueIn.reset(valueBytes.getData(), valueBytes.getPosition(),
						valueBytes.getLength());
				value = valDeserializer.deserialize(value);

				return value;
			}
		}

		/**
		 * the class for extracting the kv pair from the preserve file based on
		 * the given kv from delta file
		 * 
		 * @author yzhang
		 * 
		 */
		class WrapPreserveFile {

			IFile.PreserveFile<KEY, VALUE, SOURCEKEY, OUTVALUE> preserveFile;
			Deserializer<KEY> keyDeserializer1;
			Deserializer<VALUE> valDeserializer1;
			Deserializer<SOURCEKEY> skeyDeserializer1;
			Deserializer<OUTVALUE> outvalDeserializer1;
			DataInputBuffer keyIn1 = new DataInputBuffer();
			DataInputBuffer valueIn1 = new DataInputBuffer();
			DataInputBuffer skeyIn1 = new DataInputBuffer();
			DataInputBuffer outvalueIn1 = new DataInputBuffer();
			RawComparator<SOURCEKEY> comparator;
			boolean hasNext = true;
			boolean fileEnd = false;
			Record preserveRecord;
			int keyhash;
			boolean isEmpty = true; // signal to check whether the ivalues is
									// null, '-' remove ivalues

			public WrapPreserveFile(
					JobConf job,
					IFile.PreserveFile<KEY, VALUE, SOURCEKEY, OUTVALUE> preserveFile,
					Class<KEY> keyClass, Class<VALUE> valClass,
					Class<SOURCEKEY> skeyClass, Class<OUTVALUE> outvalClass,
					RawComparator<SOURCEKEY> com) throws IOException {
				this.preserveFile = preserveFile;
				comparator = com;
				SerializationFactory serializationFactory = new SerializationFactory(
						job);
				this.keyDeserializer1 = serializationFactory
						.getDeserializer(keyClass);
				this.keyDeserializer1.open(keyIn1);
				this.valDeserializer1 = serializationFactory
						.getDeserializer(valClass);
				this.valDeserializer1.open(valueIn1);
				this.skeyDeserializer1 = serializationFactory
						.getDeserializer(skeyClass);
				this.skeyDeserializer1.open(skeyIn1);
				this.outvalDeserializer1 = serializationFactory
						.getDeserializer(outvalClass);
				this.outvalDeserializer1.open(outvalueIn1);
			}

			public void close() throws IOException {
				preserveFile.close();
			}

			// return hashcode, conflict-avoidence hashcode
			public boolean seek(KEY k, int rehash, IntWritable hashcode)
					throws IOException {
				return preserveFile.seekKey(k, rehash, hashcode);
			}

			public Record getNextRecord(KEY k) throws IOException {
				KEY key1 = null;
				SOURCEKEY source1 = null;
				VALUE value1 = null;
				OUTVALUE outvalue1 = null;

				IFile.PreserveFile.TYPE returnType = preserveFile.next(keyIn1,
						valueIn1, skeyIn1, outvalueIn1);
				if (returnType == IFile.PreserveFile.TYPE.MORE) {
					// this might be a problem for deserializing
					// keyDeserializer1.open(keyIn1);
					key1 = keyDeserializer1.deserialize(key1);
					// valDeserializer1.open(valueIn1);
					value1 = valDeserializer1.deserialize(value1);
					// skeyDeserializer1.open(skeyIn1);
					source1 = skeyDeserializer1.deserialize(source1);

					if (key1.equals(k)) {
						preserveRecord = new Record(key1, source1, value1, null);
						// LOG.info("read preserve record: " + preserveRecord);
						return preserveRecord;
					} else {
						return null;
					}
				} else if (returnType == IFile.PreserveFile.TYPE.RECORDEND) {
					// read the preserved output key value pair
					key1 = keyDeserializer1.deserialize(key1);
					outvalue1 = outvalDeserializer1.deserialize(outvalue1);

					if (key1.equals(k)) {
						preserveRecord = new Record(key1, null, null, outvalue1);
						preserveRecord.isIntermediate = false;
						// LOG.info("read preserve record: " + preserveRecord);
						return preserveRecord;
					} else {
						return null;
					}
				} else {
					// read the end of the preservefile
					return null;
				}
			}

			public IKValues getIKValues(LinkedList<Record> updatedRecs,
					VALUE negativeV) throws IOException {

				KEY iK = updatedRecs.peek().key;
				
//				 LOG.info("updatedRecs size is " + updatedRecs.size() +
//				 " iK is " + iK);

				LinkedList<VALUE> values = new LinkedList<VALUE>();

				IFile.PreserveFile.TYPE returnType = preserveFile.next(keyIn1,
						valueIn1, skeyIn1, outvalueIn1);

				while (returnType == IFile.PreserveFile.TYPE.MORE) {
					KEY key1 = null;
					SOURCEKEY source1 = null;
					VALUE value1 = null;

					key1 = keyDeserializer1.deserialize(key1);
					value1 = valDeserializer1.deserialize(value1);
					source1 = skeyDeserializer1.deserialize(source1);

					if (key1.equals(iK)) {
//						 LOG.info("key " + key1 + " iK " + iK + " peek is " +
//						 updatedRecs.peek() + " source1 " + source1 +
//						 " updatedRecs size " + updatedRecs.size());

						if (updatedRecs.size() == 0
								|| comparator.compare(
										updatedRecs.peek().source, source1) > 0) {
							// LOG.info("put  " + value1);
							values.push(value1);

							// update the preserve file
							update(keyHashBuffer.get(), key1, value1, source1);

							// there are ivalues
							isEmpty = false;
						} else if (updatedRecs.peek().source.equals(source1)) {
							// remove this record
							if (updatedRecs.peek().value.equals(negativeV)) {
								// LOG.info("skip this record ");
								updatedRecs.poll();
							}
							// replace this record
							else {
								// replace with the delta value
								Record updatedRec = updatedRecs.poll();
								values.push(updatedRec.value);
								// LOG.info("replace this record " + value1 +
								// " with " + updatedRec.value);

								// update the preserve file
								update(keyHashBuffer.get(), updatedRec.key,
										updatedRec.value, updatedRec.source);

								// there are ivalues
								isEmpty = false;
							}
						} else if (comparator.compare(
								updatedRecs.peek().source, source1) < 0) {
							// put both delta record and presered record
							Record updatedRec = updatedRecs.poll();
							values.push(updatedRec.value);

							// LOG.info("put new record " + updatedRec.value);

							// update the preserve file
							update(keyHashBuffer.get(), updatedRec.key,
									updatedRec.value, updatedRec.source);

							// LOG.info("put  " + value1);
							values.push(value1);

							// update the preserve file
							update(keyHashBuffer.get(), key1, value1, source1);

							// there are ivalues
							isEmpty = false;
						}

						returnType = preserveFile.next(keyIn1, valueIn1,
								skeyIn1, outvalueIn1);
					} else {
						LOG.info(key1 + " is not equal to " + iK);
						// this is not the expected key, try next one
						return null;
					}
				}

				if (returnType == IFile.PreserveFile.TYPE.RECORDEND) {
					KEY key1 = null;
					OUTVALUE outvalue1 = null;

					// read the preserved output key value pair
					key1 = keyDeserializer1.deserialize(key1);
					outvalue1 = outvalDeserializer1.deserialize(outvalue1);
//					LOG.info("RECORDEND    key:\t"+key1+"   value"+outvalue1);
					if (key1.equals(iK)) {
						/*
						 * LOG.info("values are:"); for(VALUE value : values){
						 * LOG.info(value); }
						 */
						return new IKValues(key1, values, outvalue1);
					} else {
						throw new RuntimeException("read RECORDEND, " + key1
								+ " should be equal to " + iK);
					}
				} else {
					// read the end of the preservefile
					// but something seems wrong, after read all the iks, it
					// should be the recordend mark
					throw new RuntimeException("should read RECORDEND mark!");
				}
			}

			public void update(int keyhash, KEY t1, VALUE t2, SOURCEKEY t3)
					throws IOException {
				preserveFile.updateShuffleKVS(keyhash, t1, t2, t3);
			}

			public void update(int keyhash, KEY t1, OUTVALUE t4)
					throws IOException {
				// this key has no values, then remove it from index cache
				if (isEmpty) {
					preserveFile.removeKV(keyhash, t1);
				}
				// update result kv and index cache
				else {
					preserveFile.updateResKV(keyhash, t1, t4);
					isEmpty = true;
				}
			}
		}

		private boolean more = true;
		protected RawKeyValueSourceIterator in;
		private VALUE negativeV;
		private DeltaFileReader deltaReader;
		private WrapPreserveFile wrapPreserveFile;
		private RawComparator<SOURCEKEY> comparator2;
		private LinkedList<Record> deltaRecs;
		private LinkedList<LinkedList<Record> > alldeltaRecs;
		private ArrayList<Integer> keyset;
		protected Progressable reporter;
		private JobConf job;
		private IntWritable keyHashBuffer = new IntWritable();
		private OUTVALUE preservedOutValue = null;
		private IFile.PreserveFile<KEY, VALUE, SOURCEKEY, OUTVALUE> preserveWriter;

		private KEY iKey;
		private IKValues valueBuffer;

		private long totaltime;

		public ValuesInMemIterator(
				RawKeyValueSourceIterator in,
				int iteration,
				RawComparator<KEY> comparator,
				Class<KEY> keyClass,
				Class<VALUE> valClass,
				Class<SOURCEKEY> skeyClass,
				Class<OUTVALUE> outvalueClass,
				IFile.PreserveFile<KEY, VALUE, SOURCEKEY, OUTVALUE> preserveWriter,
				int taskid, VALUE negativeV, Configuration conf,
				Progressable reporter) throws IOException {
			this.negativeV = negativeV;
			this.in = in;
			this.reporter = reporter;
			job = (JobConf) conf;

			comparator2 = (RawComparator<SOURCEKEY>) WritableComparator.get(job
					.getStaticKeyClass().asSubclass(WritableComparable.class));

			this.deltaReader = new DeltaFileReader(in, job, keyClass, valClass,
					skeyClass, negativeV);
			
//			deltaRecs = deltaReader.getRecords();
			keyset=new ArrayList<Integer>();
			alldeltaRecs = new LinkedList<LinkedList<Record>>();
			while((deltaRecs = deltaReader.getRecords())!=null && alldeltaRecs.size()<81920){
				alldeltaRecs.add(deltaRecs);
				keyset.add(deltaRecs.peek().key.hashCode());
//				LOG.info(deltaRecs.peek().key+"   "+deltaRecs.size()+"   "+alldeltaRecs.size()); 
			} 
			this.preserveWriter=preserveWriter;
			preserveWriter.setKeyset(keyset);

			wrapPreserveFile = new WrapPreserveFile(job, preserveWriter,
					keyClass, valClass, skeyClass, outvalueClass, comparator2);
			deltaRecs = alldeltaRecs.poll();
			
			if (deltaRecs == null) {
				LOG.info("no entries in delta file!!!");
				more = false;
			} else {
				/**
				 * the returned linked list is for the same key, sort the linked
				 * list based on source as the second key
				 */
				Collections.sort(deltaRecs, new Comparator<Record>() {
					@Override
					public int compare(Record o1, Record o2) {
						return job.getOutputKeyComparator().compare(o1.source,
								o2.source);
					}
				});

				/**
				 * handle the hash conflict, if there is a hash conflict, trial
				 * will be increased by 1
				 */
				int trial = 0;
				while (valueBuffer == null
						&& wrapPreserveFile.seek(deltaRecs.peek().key, trial,
								keyHashBuffer)) {
					trial++;
					// if the returned record's key is not the delta key (might
					// be hash conflict), then try again in the while loop
					valueBuffer = wrapPreserveFile.getIKValues(deltaRecs,
							negativeV);

					// LOG.info("record: " + deltaRecs.peek().key + " size " +
					// valueBuffer.ivalues.size());
				}

				if (trial == 0)
					throw new RuntimeException("no entries in preserve file!!!");
				// LOG.info("initial phase:  to match " + currDeltaRecord.key +
				// " is " + currPreserveRecord);
			}
		}

		// / Iterator methods
		public boolean hasNext() {
			return valueBuffer.ivalues.size() != 0;
		}

		private int ctr = 0;

		/**
		 * Shimin:read all the reduce input kvs with the same key together, and
		 * return kv one-by-one Yanfeng: but has memory-overflow risk Let's try
		 * it first and compare their performance
		 */

		/**
		 * zhangyf has changed the programming model of the next method. In this
		 * next() method, it might return a negative value users set, then the
		 * user's program should take care of this negative value and skip it.
		 */
		public VALUE next() {
			reporter.progress();
			return valueBuffer.ivalues.poll();
		}

		public void remove() {
			throw new RuntimeException("not implemented");
		}

		public OUTVALUE getPreservedOutValue() {
			return valueBuffer.ovalue;
		}

		public void updateResKV(KEY key, OUTVALUE outvalue) throws IOException {
			wrapPreserveFile.update(keyHashBuffer.get(), key, outvalue);
			// LOG.info("update reskv " + key + " hashcode " +
			// keyHashBuffer.get());
		}

		/** Start processing next unique key. */
		public void nextKey() throws IOException {

			boolean seek_success = false;

			while (!seek_success) {
//				deltaRecs = deltaReader.getRecords(); 
				if(alldeltaRecs.size() == 0){
					alldeltaRecs.clear();
					keyset.clear();
					while((deltaRecs = deltaReader.getRecords())!=null && alldeltaRecs.size()<81920){ 
						alldeltaRecs.add(deltaRecs);
						keyset.add(deltaRecs.peek().key.hashCode());
//						LOG.info(deltaRecs.peek().key+"   "+deltaRecs.size()+"   "+alldeltaRecs.size()); 
					} 
					preserveWriter.setKeyset(keyset);
					preserveWriter.setHasanalysekey(false);
				}
				deltaRecs = alldeltaRecs.poll();
			 
				
				if (deltaRecs == null) {
					more = false;
					return;
				}

				/**
				 * the returned linked list is for the same key, sort the linked
				 * list based on source as the second key
				 */
				Collections.sort(deltaRecs, new Comparator<Record>() {
					@Override
					public int compare(Record o1, Record o2) {
						return job.getOutputKeyComparator().compare(o1.source,
								o2.source);
					}
				});

				valueBuffer = null;
				int trial = 0;
				// long time1 = System.currentTimeMillis();
				while (valueBuffer == null
						&& (seek_success = wrapPreserveFile.seek(
								deltaRecs.peek().key, trial, keyHashBuffer))) {
					trial++;
					// if the returned record's key is not the delta key (might
					// be hash conflict), then try again in the while loop
					valueBuffer = wrapPreserveFile.getIKValues(deltaRecs,
							negativeV);
				}
				if (trial > 1) {
					LOG.info("seek key has hash conflict hash is "
							+ keyHashBuffer.get());
				}

				// long time2 = System.currentTimeMillis();
				// totaltime += time2-time1;
			}

			++ctr;
		}

		/** True iff more keys remain. */
		public boolean more() {
			return more;
		}

		/** The current key. */
		public KEY getKey() {
			return valueBuffer.iKey;
		}

		public void close() throws IOException {
			wrapPreserveFile.close();

			LOG.info("total getIKValues time is " + totaltime);
		}
	}

	private class SkippingReduceValuesIterator<KEY, VALUE> extends
			ReduceValuesIterator<KEY, VALUE> {
		private SkipRangeIterator skipIt;
		private TaskUmbilicalProtocol umbilical;
		private Counters.Counter skipGroupCounter;
		private Counters.Counter skipRecCounter;
		private long grpIndex = -1;
		private Class<KEY> keyClass;
		private Class<VALUE> valClass;
		private SequenceFile.Writer skipWriter;
		private boolean toWriteSkipRecs;
		private boolean hasNext;
		private TaskReporter reporter;

		public SkippingReduceValuesIterator(RawKeyValueIterator in,
				RawComparator<KEY> comparator, Class<KEY> keyClass,
				Class<VALUE> valClass, Configuration conf,
				TaskReporter reporter, TaskUmbilicalProtocol umbilical)
				throws IOException {
			super(in, comparator, keyClass, valClass, conf, reporter);
			this.umbilical = umbilical;
			this.skipGroupCounter = reporter
					.getCounter(Counter.REDUCE_SKIPPED_GROUPS);
			this.skipRecCounter = reporter
					.getCounter(Counter.REDUCE_SKIPPED_RECORDS);
			this.toWriteSkipRecs = toWriteSkipRecs()
					&& SkipBadRecords.getSkipOutputPath(conf) != null;
			this.keyClass = keyClass;
			this.valClass = valClass;
			this.reporter = reporter;
			skipIt = getSkipRanges().skipRangeIterator();
			mayBeSkip();
		}

		void nextKey() throws IOException {
			super.nextKey();
			mayBeSkip();
		}

		boolean more() {
			return super.more() && hasNext;
		}

		private void mayBeSkip() throws IOException {
			hasNext = skipIt.hasNext();
			if (!hasNext) {
				LOG.warn("Further groups got skipped.");
				return;
			}
			grpIndex++;
			long nextGrpIndex = skipIt.next();
			long skip = 0;
			long skipRec = 0;
			while (grpIndex < nextGrpIndex && super.more()) {
				while (hasNext()) {
					VALUE value = moveToNext();
					if (toWriteSkipRecs) {
						writeSkippedRec(getKey(), value);
					}
					skipRec++;
				}
				super.nextKey();
				grpIndex++;
				skip++;
			}

			// close the skip writer once all the ranges are skipped
			if (skip > 0 && skipIt.skippedAllRanges() && skipWriter != null) {
				skipWriter.close();
			}
			skipGroupCounter.increment(skip);
			skipRecCounter.increment(skipRec);
			reportNextRecordRange(umbilical, grpIndex);
		}

		@SuppressWarnings("unchecked")
		private void writeSkippedRec(KEY key, VALUE value) throws IOException {
			if (skipWriter == null) {
				Path skipDir = SkipBadRecords.getSkipOutputPath(conf);
				Path skipFile = new Path(skipDir, getTaskID().toString());
				skipWriter = SequenceFile.createWriter(
						skipFile.getFileSystem(conf), conf, skipFile, keyClass,
						valClass, CompressionType.BLOCK, reporter);
			}
			skipWriter.append(key, value);
		}
	}

	private class CPCReduceValuesIterator<KEY, VALUE, OUTKEY, OUTVALUE> extends
			ValuesIterator<KEY, VALUE> {
		
		private RecordReader<OUTKEY, OUTVALUE> lastFilterReader;
		private OUTKEY lastResultKey;
		private OUTVALUE lastResultValue;
		
		public CPCReduceValuesIterator(RawKeyValueIterator in,
				RawComparator<KEY> comparator, Class<KEY> keyClass,
				Class<VALUE> valClass, Configuration conf, int iteration, 
				TaskReporter reporter)
				throws IOException {
			super(in, comparator, keyClass, valClass, conf, reporter);
			
			//get last iteration filterout reader
			FileSystem lfs = FileSystem.getLocal(conf);
			Path lastresult = new Path(conf.get("mapred.incr.tmp", "/mnt/iteroop/")
						+ ((JobConf)conf).getIterativeAlgorithmID() + "/filter-"
						+ (iteration - 1) + "."
						+ getTaskID().getTaskID().getId());
			long filelen = lfs.getFileStatus(lastresult).getLen();
			InputSplit inputSplit = new FileSplit(lastresult, 0, filelen, null,
					true);
			lastFilterReader = ((JobConf)conf).getResultInputFormat().getRecordReader(inputSplit,
					((JobConf)conf), reporter);
			lastResultKey = lastFilterReader.createKey();
			lastResultValue = lastFilterReader.createValue();
		}
		
		void nextKey() throws IOException {
			super.nextKey();
			lastFilterReader.next(lastResultKey, lastResultValue);
			while(!(lastResultKey == this.getKey())){
				super.nextKey();
			}
		}
		
		@Override
		public VALUE next() {
			reduceInputValueCounter.increment(1);
			return moveToNext();
		}
		
		protected VALUE moveToNext() {
			return super.next();
		}
		
		public void informReduceProgress() {
			reducePhase.set(super.in.getProgress().get()); // update progress
			reporter.progress();
		}
	}

	
	@Override
	@SuppressWarnings("unchecked")
	public void run(JobConf job, final TaskUmbilicalProtocol umbilical)
			throws IOException, InterruptedException, ClassNotFoundException {
		this.umbilical = umbilical;
		job.setBoolean("mapred.skip.on", isSkipping());

		if (isMapOrReduce()) {
			copyPhase = getProgress().addPhase("copy");
			sortPhase = getProgress().addPhase("sort");
			reducePhase = getProgress().addPhase("reduce");
		}
		// start thread that will handle communication with parent
		TaskReporter reporter = new TaskReporter(getProgress(), umbilical,
				jvmContext);
		reporter.startCommunicationThread();
		boolean useNewApi = job.getUseNewReducer();
		initialize(job, getJobID(), reporter, useNewApi);

		// check if it is a cleanupJobTask
		if (jobCleanup) {
			runJobCleanupTask(umbilical, reporter);
			return;
		}
		if (jobSetup) {
			runJobSetupTask(umbilical, reporter);
			return;
		}
		if (taskCleanup) {
			runTaskCleanupTask(umbilical, reporter);
			return;
		}

		// Initialize the codec
		codec = initCodec();
		boolean isLocal = "local"
				.equals(job.get("mapred.job.tracker", "local"));

		hdfs = FileSystem.get(job);
		localfs = FileSystem.getLocal(job);

		if(job.isIterative() || job.isIterCPC() || 
				job.isIncrementalIterative() || job.isIncrMRBGOnly()){
			long starttime = System.currentTimeMillis();

			// create a new thread to control when to start
			MapOutputReadyChecker mapchecker = new MapOutputReadyChecker(
					umbilical, this, iteration+1);
			mapchecker.setDaemon(true);
			mapchecker.start();

			int maxiteration = job.getMaxIterations();

			synchronized (this) {
				try {
					LOG.info("start waiting... for iteration 1 ");
					this.wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			try {
				while (++iteration <= maxiteration) {
					LOG.info("start iteration " + iteration);
					long time1 = System.currentTimeMillis();

					copyPhase = getProgress().addPhase("copy");
					sortPhase = getProgress().addPhase("sort");
					reducePhase = getProgress().addPhase("reduce");

					if (!isLocal) {
						reduceCopier = new ReduceCopier(umbilical, job,
								reporter);
						if (!reduceCopier.fetchOutputs()) {
							if (reduceCopier.mergeThrowable instanceof FSError) {
								throw (FSError) reduceCopier.mergeThrowable;
							}
							throw new IOException("Task: " + getTaskID()
									+ " - The reduce copier failed",
									reduceCopier.mergeThrowable);
						}
					}
					copyPhase.complete(); // copy is already complete
					long time2 = System.currentTimeMillis();
					setPhase(TaskStatus.Phase.SORT);
					statusUpdate(umbilical);

					final FileSystem rfs = FileSystem.getLocal(job).getRaw();

					RawKeyValueIterator rIter = null;

					if (job.isIterative() || job.isIterCPC()) {
						rIter = isLocal ? Merger.merge(job, rfs, job
								.getMapOutputKeyClass(), job
								.getMapOutputValueClass(), codec,
								getMapFiles(rfs, true), !conf
										.getKeepFailedTaskFiles(), job.getInt(
										"io.sort.factor", 100), new Path(
										getTaskID().toString()), job
										.getOutputKeyComparator(), reporter,
								spilledRecordsCounter, null) : reduceCopier
								.createKVIterator(job, rfs, reporter);
					} else {
						rIter = isLocal ? Merger.merge(job, rfs, job
								.getMapOutputKeyClass(), job
								.getMapOutputValueClass(), codec,
								getMapFiles(rfs, true), !conf
										.getKeepFailedTaskFiles(), job.getInt(
										"io.sort.factor", 100), new Path(
										getTaskID().toString()), job
										.getOutputKeyComparator(), reporter,
								spilledRecordsCounter, null) : reduceCopier
								.createKVSIterator(job, rfs, reporter);
					} 

					// free up the data structures
					mapOutputFilesOnDisk.clear();

					sortPhase.complete(); // sort is complete

					long time3 = System.currentTimeMillis();

					setPhase(TaskStatus.Phase.REDUCE);
					statusUpdate(umbilical);
					Class keyClass = job.getMapOutputKeyClass();
					Class valueClass = job.getMapOutputValueClass();
					RawComparator comparator = job
							.getOutputValueGroupingComparator();

					/*
					 * RawKeyValueSourceIterator preserveIter = null;
					 * if(IsPreserveMore){ preserveIter =
					 * reduceCopier.createPreserveKVSIterator2(job, reporter);
					 * }else{ preserveIter =
					 * reduceCopier.createPreserveKVSIterator(job, iteration,
					 * reporter); }
					 */
					if (job.isIterative()) {
						runIterativeReducer(job, umbilical, reporter,
								iteration, rIter, comparator, keyClass,
								valueClass);
					} else if(job.isIterCPC()){
						runIterCPCReducer(job, umbilical, reporter,
								iteration, rIter, comparator, keyClass,
								valueClass);
					} else if(job.isIncrMRBGOnly()){
						runIncrMRBGReducer(job, umbilical,
								reporter, iteration,
								(RawKeyValueSourceIterator) rIter, comparator,
								keyClass, valueClass, job.getStaticKeyClass(),
								job.getOutputValueClass(), starttime);
					} else if (job.isIncrementalIterative()) {
						runIncrementalIterativeReducer(job, umbilical,
								reporter, iteration,
								(RawKeyValueSourceIterator) rIter, comparator,
								keyClass, valueClass, job.getStaticKeyClass(),
								job.getOutputValueClass(), starttime);
					}

					//iterationDone(umbilical, reporter, iteration);
					
					long time4 = System.currentTimeMillis();

					System.out.println("iteration " + iteration + " reduce task "
							+ this.getTaskID().getTaskID().getId() + " takes "
							+ (time4 - time1) + " copy " + (time2 - time1)
							+ " sort " + (time3 - time2) + " reduce "
							+ (time4 - time3) + " total " + (time4 - starttime));

					if (iteration + 1 > maxiteration)
						break;

					synchronized (this) {
						try {
							LOG.info("start waiting... ");

							this.wait();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}

			} finally {
				mapchecker.interrupt();
				mapchecker = null;
			}
		}
		//other cases, 1.normal 2.preserve 3.incrstart
		else{
			long time1 = System.currentTimeMillis();
			if (!isLocal) {
				reduceCopier = new ReduceCopier(umbilical, job, reporter);
				if (!reduceCopier.fetchOutputs()) {
					if (reduceCopier.mergeThrowable instanceof FSError) {
						throw (FSError) reduceCopier.mergeThrowable;
					}
					throw new IOException("Task: " + getTaskID()
							+ " - The reduce copier failed",
							reduceCopier.mergeThrowable);
				}
			}
			copyPhase.complete(); // copy is already complete
			long time2 = System.currentTimeMillis();

			setPhase(TaskStatus.Phase.SORT);
			statusUpdate(umbilical);

			final FileSystem rfs = FileSystem.getLocal(job).getRaw();

			RawKeyValueIterator rIter = isLocal ? Merger.merge(job, rfs, job
					.getMapOutputKeyClass(), job.getMapOutputValueClass(),
					codec, getMapFiles(rfs, true), !conf
							.getKeepFailedTaskFiles(), job.getInt(
							"io.sort.factor", 100), new Path(getTaskID()
							.toString()), job.getOutputKeyComparator(),
					reporter, spilledRecordsCounter, null)
					: (job.isPreserve() || job.isIncrementalStart()) ? reduceCopier
							.createKVSIterator(job, rfs, reporter)
							: reduceCopier.createKVIterator(job, rfs, reporter);

			// free up the data structures
			mapOutputFilesOnDisk.clear();

			sortPhase.complete(); // sort is complete

			long time3 = System.currentTimeMillis();

			setPhase(TaskStatus.Phase.REDUCE);
			statusUpdate(umbilical);
			Class keyClass = job.getMapOutputKeyClass();
			Class valueClass = job.getMapOutputValueClass();
			Class skeyClass = job.getSourceKeyClass();
			RawComparator comparator = job.getOutputValueGroupingComparator();

			if (job.isPreserve()) {
				runPreserveReducer(job, umbilical, reporter,
						(RawKeyValueSourceIterator) rIter, comparator,
						keyClass, valueClass, job.getStaticKeyClass(),
						job.getOutputValueClass());
			} else if (job.isIncrementalStart()) {
				/*
				 * RawKeyValueSourceIterator preserveIter = null;
				 * if(IsPreserveMore){ preserveIter =
				 * reduceCopier.createPreserveKVSIterator2(job, reporter);
				 * }else{ preserveIter =
				 * reduceCopier.createPreserveKVSIterator(job, 0, reporter); //0
				 * no use }
				 */
				runIncrementalReducer(job, umbilical, reporter,
						(RawKeyValueSourceIterator) rIter, comparator,
						keyClass, valueClass, job.getStaticKeyClass(),
						job.getOutputValueClass());
			} else if (useNewApi) {
				runNewReducer(job, umbilical, reporter, rIter, comparator,
						keyClass, valueClass);
			} else if (job.isEasyPreserve()) {
				runBatchPreserveReducer(job, umbilical, reporter, rIter,
						comparator, keyClass, valueClass);
			} else if (job.isEasyIncremental()) {
				runBatchIncrementalReducer(job, umbilical, reporter, rIter,
						comparator, keyClass, valueClass);
			} else {
				runOldReducer(job, umbilical, reporter, rIter, comparator,
						keyClass, valueClass);
			}

			long time4 = System.currentTimeMillis();

			System.out.println("job " + job.get("mapred.job.id") + " map task "
					+ this.getTaskID().getTaskID().getId()
					+ " takes copy phase " + (time2 - time1)
					+ " ms sort phase " + (time3 - time2) + " ms reduce phase "
					+ (time4 - time3) + " ms");
		}
		// the common case, for the case that is not incremental iterative app

		done(umbilical, reporter);
	}

	private class OldTrackingRecordWriter<K, V> implements RecordWriter<K, V> {

		private final RecordWriter<K, V> real;
		private final org.apache.hadoop.mapred.Counters.Counter outputRecordCounter;
		private final org.apache.hadoop.mapred.Counters.Counter fileOutputByteCounter;
		private final Statistics fsStats;

		public OldTrackingRecordWriter(
				org.apache.hadoop.mapred.Counters.Counter outputRecordCounter,
				JobConf job, TaskReporter reporter, String finalName,
				boolean onhdfs) throws IOException {
			this.outputRecordCounter = outputRecordCounter;
			this.fileOutputByteCounter = reporter
					.getCounter(FileOutputFormat.Counter.BYTES_WRITTEN);
			Statistics matchedStats = null;
			if (job.getOutputFormat() instanceof FileOutputFormat) {
				matchedStats = getFsStatistics(
						FileOutputFormat.getOutputPath(job), job);
			}
			fsStats = matchedStats;
			long bytesOutPrev = getOutputBytes(fsStats);

			if (onhdfs) {
				// for hdfs reduce output
				FileSystem fs = FileSystem.get(job);
				this.real = job.getOutputFormat().getRecordWriter(fs, job,
						finalName, reporter);
			} else {
				// for local reduce output
				/*
				 * FileSystem localfs = FileSystem.getLocal(job);
				 * FSDataOutputStream fileOut = localfs.create(new
				 * Path(finalName), reporter); String keyValueSeparator =
				 * job.get("mapred.textoutputformat.separator", "\t"); this.real
				 * = new LineRecordWriter<K, V>(fileOut, keyValueSeparator);
				 * //cannot use job.getOutputFormat().getRecordWriter(),
				 * LineRecordWriter can write local files
				 */
				FileSystem localfs = FileSystem.getLocal(job);
				LOG.info("I am creating " + finalName);
				this.real = job.getOutputFormat().getRecordWriter(localfs, job,
						finalName, reporter);
				// LOG.info("created file length " + localfs.getFileStatus(new
				// Path(finalName)).getLen());
			}

			long bytesOutCurr = getOutputBytes(fsStats);
			fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
		}

		@Override
		public void write(K key, V value) throws IOException {
			long bytesOutPrev = getOutputBytes(fsStats);
			real.write(key, value);
			long bytesOutCurr = getOutputBytes(fsStats);
			fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
			outputRecordCounter.increment(1);
		}

		@Override
		public void close(Reporter reporter) throws IOException {
			long bytesOutPrev = getOutputBytes(fsStats);
			real.close(reporter);
			long bytesOutCurr = getOutputBytes(fsStats);
			fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
		}

		private long getOutputBytes(Statistics stats) {
			return stats == null ? 0 : stats.getBytesWritten();
		}
	}

	private class OldTrackingIFileRecordWriter<K, V> implements
			RecordWriter<K, V> {

		private final IFile.Writer<K, V> real;
		private final org.apache.hadoop.mapred.Counters.Counter outputRecordCounter;
		private final org.apache.hadoop.mapred.Counters.Counter fileOutputByteCounter;
		private final Statistics fsStats;
		private final Class<K> keyclass;
		private final Class<V> valclass;

		public OldTrackingIFileRecordWriter(
				org.apache.hadoop.mapred.Counters.Counter outputRecordCounter,
				JobConf job, TaskReporter reporter, String finalName,
				boolean onhdfs) throws IOException {
			this.outputRecordCounter = outputRecordCounter;
			this.fileOutputByteCounter = reporter
					.getCounter(FileOutputFormat.Counter.BYTES_WRITTEN);
			Statistics matchedStats = null;
			if (job.getOutputFormat() instanceof FileOutputFormat) {
				matchedStats = getFsStatistics(
						FileOutputFormat.getOutputPath(job), job);
			}
			fsStats = matchedStats;
			long bytesOutPrev = getOutputBytes(fsStats);
			keyclass = (Class<K>) job.getOutputKeyClass();
			valclass = (Class<V>) job.getOutputValueClass();

			if (onhdfs) {
				// for hdfs reduce output
				FileSystem hdfs = FileSystem.get(job);
				FSDataOutputStream out = hdfs.create(new Path(finalName));
				this.real = new IFile.Writer<K, V>(job, out, keyclass,
						valclass, null, outputRecordCounter);
			} else {
				// for local reduce output
				FileSystem lfs = FileSystem.getLocal(job);
				FSDataOutputStream out = lfs.create(new Path(finalName));
				this.real = new IFile.Writer<K, V>(job, out, keyclass,
						valclass, null, outputRecordCounter);
			}

			long bytesOutCurr = getOutputBytes(fsStats);
			fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
		}

		@Override
		public void write(K key, V value) throws IOException {
			long bytesOutPrev = getOutputBytes(fsStats);
			real.append(key, value);
			long bytesOutCurr = getOutputBytes(fsStats);
			fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
			outputRecordCounter.increment(1);
		}

		@Override
		public void close(Reporter reporter) throws IOException {
			long bytesOutPrev = getOutputBytes(fsStats);
			real.close();
			long bytesOutCurr = getOutputBytes(fsStats);
			fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
		}

		private long getOutputBytes(Statistics stats) {
			return stats == null ? 0 : stats.getBytesWritten();
		}
	}

	private class WrappedOutputLocalCollector<INKEY, INVALUE, OUTKEY, OUTVALUE>
			implements OutputCollector<OUTKEY, OUTVALUE> {
		private RecordWriter<OUTKEY, OUTVALUE> localwriter;
		private TaskReporter reporter;
		private TerminateChecker<INKEY, INVALUE, OUTKEY, OUTVALUE> termchecker;
		private RecordReader<OUTKEY, OUTVALUE> lastResultReader;
		private boolean termcheck;
		private OUTKEY lastResultKey;
		private OUTVALUE lastResultValue;
		private OUTVALUE collectedValue;

		public WrappedOutputLocalCollector(
				RecordWriter<OUTKEY, OUTVALUE> localwriter,
				TaskReporter inreporter,
				TerminateChecker<INKEY, INVALUE, OUTKEY, OUTVALUE> intermchecker)
				throws IOException {
			this.localwriter = localwriter;
			reporter = inreporter;
			termchecker = intermchecker;
			termcheck = (intermchecker == null) ? false : true;
			if (termcheck) {
				lastResultReader = termchecker.getTermCheckReader();
				lastResultKey = lastResultReader.createKey();
				lastResultValue = lastResultReader.createValue();
			}
		}

		@Override
		public void collect(OUTKEY key, OUTVALUE value) throws IOException {
			// LOG.info("collect " + key + "\t" + value);
			localwriter.write(key, value);
			collectedValue = value;
			reporter.progress();

			if (termcheck) {
				lastResultReader.next(lastResultKey, lastResultValue);
				if (!lastResultKey.equals(key)) {
					throw new IOException(
							"Keys do not match during termination check! Old Key: "
									+ lastResultKey + " New Key: " + key);
				} else {
					termchecker.accumulateDistance(key, lastResultValue, value);
				}
			}
		}

		public OUTVALUE collectedValue() {
			return collectedValue;
		}

		public void clearCollectedValue() {
			collectedValue = null;
		}
	}

	private class WrappedFilteredOutputLocalCollector<INKEY, INVALUE, OUTKEY, OUTVALUE>
			implements OutputCollector<OUTKEY, OUTVALUE> {
		private RecordWriter<OUTKEY, OUTVALUE> localwriter;
		private RecordWriter<OUTKEY, OUTVALUE> filterwriter;
		private TaskReporter reporter;
		private TerminateChecker<INKEY, INVALUE, OUTKEY, OUTVALUE> termchecker;
		private RecordReader<OUTKEY, OUTVALUE> lastResultReader;
		private OUTKEY lastResultKey;
		private OUTVALUE lastResultValue;
		private OUTVALUE collectedValue;
		private float filter_threshold;
		private int num_nonconv = 0;
		private RawComparator<INKEY> comparator;
		private int iteration;
		
		public WrappedFilteredOutputLocalCollector(
				RecordWriter<OUTKEY, OUTVALUE> localwriter,
				RecordWriter<OUTKEY, OUTVALUE> filterwriter,
				TaskReporter inreporter,
				TerminateChecker<INKEY, INVALUE, OUTKEY, OUTVALUE> termchecker,
				RecordReader<OUTKEY, OUTVALUE> lastResultReader,
				float filter_threshold, RawComparator<INKEY> comparator, int iteration)
				throws IOException {
			this.localwriter = localwriter;
			this.filterwriter = filterwriter;
			reporter = inreporter;
			this.termchecker = termchecker;
			this.lastResultReader = lastResultReader;
			lastResultKey = lastResultReader.createKey();
			lastResultValue = lastResultReader.createValue();
			lastResultReader.next(lastResultKey, lastResultValue);
			this.filter_threshold = filter_threshold;
			this.comparator = comparator;
			this.iteration = iteration;
		}
		
		@Override
		public void collect(OUTKEY key, OUTVALUE value) throws IOException {
			//LOG.info("collect " + key + "\t" + value);
			
			//if iteration less than 5, we don't need filter
			if(iteration < 5){
				filterwriter.write(key, value);
				num_nonconv++;
				return;
			}
			
			while(comparator.compare((INKEY)lastResultKey, (INKEY)key)< 0){
				//LOG.info("lastResultReader " + lastResultKey + "\t" + lastResultValue);
				lastResultReader.next(lastResultKey, lastResultValue);
			}
					
			if(lastResultKey.equals(key)){
				if(termchecker.getSingleDistance(key, lastResultValue, value) > filter_threshold){
					filterwriter.write(key, value);
					num_nonconv++;
				}else{
					localwriter.write(key, value);
					lastResultReader.next(lastResultKey, lastResultValue);
					
					//LOG.info("lastResultReader replace " + lastResultKey + "\t" + lastResultValue);
				}
			}
			
			
			reporter.progress();
		}
		
		public int getNonConvNum(){
			return num_nonconv;
		}
	}
	
	private class WrappedIndexOutputLocalCollector<INKEY, INVALUE, OUTKEY, OUTVALUE>
			implements OutputCollector<OUTKEY, OUTVALUE> {
		private TaskReporter reporter;
		private TerminateChecker<INKEY, INVALUE, OUTKEY, OUTVALUE> termchecker;
		private RecordReader<OUTKEY, OUTVALUE> lastResultReader;
		private boolean termcheck;
		private OUTKEY lastResultKey;
		private OUTVALUE lastResultValue;
		private OUTKEY collectedKey;
		private OUTVALUE collectedValue;
		private RecordWriter<OUTKEY, OUTVALUE> out;

		public WrappedIndexOutputLocalCollector(TaskReporter inreporter,
				TerminateChecker<INKEY, INVALUE, OUTKEY, OUTVALUE> intermchecker)
				throws IOException {
			reporter = inreporter;
			termchecker = intermchecker;
			termcheck = (intermchecker == null) ? false : true;
			if (termcheck) {
				lastResultReader = termchecker.getTermCheckReader();
				lastResultKey = lastResultReader.createKey();
				lastResultValue = lastResultReader.createValue();
			}
		}

		public WrappedIndexOutputLocalCollector(
				TaskReporter inreporter,
				TerminateChecker<INKEY, INVALUE, OUTKEY, OUTVALUE> intermchecker,
				RecordWriter<OUTKEY, OUTVALUE> out) throws IOException {
			reporter = inreporter;
			termchecker = intermchecker;
			termcheck = (intermchecker == null) ? false : true;
			if (termcheck) {
				lastResultReader = termchecker.getTermCheckReader();
				lastResultKey = lastResultReader.createKey();
				lastResultValue = lastResultReader.createValue();
			}
			this.out = out;
		}

		@Override
		public void collect(OUTKEY key, OUTVALUE value) throws IOException {
			// LOG.info("collect " + key + "\t" + value);
			collectedValue = value;
			collectedKey = key;
			reporter.progress();

			if (termcheck) {
				lastResultReader.next(lastResultKey, lastResultValue);
				if (!lastResultKey.equals(key)) {
					throw new IOException(
							"Keys do not match during termination check! Old Key: "
									+ lastResultKey + " New Key: " + key);
				} else {
					termchecker.accumulateDistance(key, lastResultValue, value);
				}
			}
			if (out != null) {
				LOG.info("out   key: " + key + "\tvalue: " + value);
				out.write(key, value);
			}
		}

		public OUTKEY collectedKey() {
			return collectedKey;
		}

		public OUTVALUE collectedValue() {
			return collectedValue;
		}

		public void clearCollectedValue() {
			collectedValue = null;
		}
	}

	private class FilterOutputLocalCollector<INKEY, INVALUE, OUTKEY, OUTVALUE extends Writable>
			implements OutputCollector<OUTKEY, OUTVALUE> {
		private RecordWriter<OUTKEY, OUTVALUE> localwriter;
		private RecordWriter<OUTKEY, OUTVALUE> filterwriter;
		private TaskReporter reporter;
		private ResultFileQueue<INKEY, INVALUE, OUTKEY, OUTVALUE> resultQueue;
		private IterativeReducer<INKEY, INVALUE, OUTKEY, OUTVALUE> reducer;
		private OUTKEY lastResultKey;
		private OUTVALUE latestValue;
		private RawComparator<OUTKEY> comparator;
		private boolean more;
		private int nonConvergedItems = 0;

		// for single preserve file
		private boolean bwrite = true;

		public FilterOutputLocalCollector(
				RecordWriter<OUTKEY, OUTVALUE> localwriter,
				RecordWriter<OUTKEY, OUTVALUE> filterwriter,
				TaskReporter inreporter,
				ResultFileQueue<INKEY, INVALUE, OUTKEY, OUTVALUE> inResultQueue)
				throws IOException {
			this.localwriter = localwriter;
			this.filterwriter = filterwriter;
			reporter = inreporter;

			comparator = conf.getOutputKeyComparator();

			reducer = ReflectionUtils.newInstance(
					conf.getIterativeReducerClass(), conf);

			resultQueue = inResultQueue;
			more = resultQueue.next();
			if (!more)
				throw new RuntimeException("no more data to read!!!");
			lastResultKey = resultQueue.getKey();
			latestValue = WritableUtils.clone(resultQueue.getValue(), conf);
		}

		@Override
		public void collect(OUTKEY key, OUTVALUE value) throws IOException {

			reporter.progress();

			// LOG.info("last result key is " + lastResultKey);
			while (more && comparator.compare(lastResultKey, key) < 0) {

				if (bwrite)
					localwriter.write(lastResultKey, resultQueue.getValue());

				// LOG.info("last result key is " + lastResultKey);
				more = resultQueue.next();
				if (!more)
					break;

				lastResultKey = resultQueue.getKey();
				bwrite = true;
			}

			if (comparator.compare(lastResultKey, key) != 0) {
				throw new RuntimeException(
						"no key "
								+ key
								+ " in last result can be found!!! or new key (not considered yet) !!!");
			}

			latestValue = WritableUtils.clone(resultQueue.getValue(), conf);

			float diff = reducer.distance(key, latestValue, value);

			// LOG.info("for key " + key + " source file " + largestPri +
			// " diff between " + latestValue + " and " + value + " is " +
			// diff);

			if (diff >= conf.getFilterThreshold()) {
				// localwriter.write(key, value);
				filterwriter.write(key, value);
				// LOG.info("collect " + key + "\t" + value + " diff " + diff);
				nonConvergedItems++;
			} else {
				// LOG.info("skip " + key + "\t" + value + " diff " + diff);
			}

			// for single preserve file
			localwriter.write(key, value);
			// avoid write twice
			bwrite = false;
		}

		public void writeRestKVs() throws IOException {
			reporter.progress();

			// LOG.info("last result key is " + lastResultKey);
			while (resultQueue.next()) {
				localwriter.write(resultQueue.getKey(), resultQueue.getValue());
			}
		}

		public int getNonConvergedItems() {
			return nonConvergedItems;
		}
	}

	private class WrappedOutputBothCollector<INKEY, INVALUE, OUTKEY, OUTVALUE>
			implements OutputCollector<OUTKEY, OUTVALUE> {
		private RecordWriter<OUTKEY, OUTVALUE> localwriter;
		private RecordWriter<OUTKEY, OUTVALUE> hdfswriter;
		private TaskReporter reporter;
		private TerminateChecker<INKEY, INVALUE, OUTKEY, OUTVALUE> termchecker;
		private RecordReader<OUTKEY, OUTVALUE> lastResultReader;
		private boolean termcheck;
		private OUTKEY lastResultKey;
		private OUTVALUE lastResultValue;

		public WrappedOutputBothCollector(
				RecordWriter<OUTKEY, OUTVALUE> localwriter,
				RecordWriter<OUTKEY, OUTVALUE> hdfswriter,
				TaskReporter inreporter,
				TerminateChecker<INKEY, INVALUE, OUTKEY, OUTVALUE> intermchecker)
				throws IOException {
			this.localwriter = localwriter;
			this.hdfswriter = hdfswriter;
			reporter = inreporter;
			termchecker = intermchecker;
			termcheck = (intermchecker == null) ? false : true;
			if (termcheck) {
				lastResultReader = termchecker.getTermCheckReader();
				lastResultKey = lastResultReader.createKey();
				lastResultValue = lastResultReader.createValue();
			}
		}

		@Override
		public void collect(OUTKEY key, OUTVALUE value) throws IOException {
			// LOG.info("collect " + key + "\t" + value);
			localwriter.write(key, value);
			hdfswriter.write(key, value);
			reporter.progress();

			if (termcheck) {
				lastResultReader.next(lastResultKey, lastResultValue);
				if (!lastResultKey.equals(key)) {
					throw new IOException(
							"Keys do not match during termination check! Old Key: "
									+ lastResultKey + " New Key: " + key);
				} else {
					termchecker.accumulateDistance(key, lastResultValue, value);
				}
			}
		}
	}

	private class TerminateChecker<INKEY, INVALUE, OUTKEY, OUTVALUE> {
		private RecordReader<OUTKEY, OUTVALUE> reader;
		private IterativeReducer<INKEY, INVALUE, OUTKEY, OUTVALUE> reducer;
		private float distance = 0;

		public TerminateChecker(int currIteration, Reporter reporter)
				throws IOException {
			reader = getLastResultRecordReader(currIteration, reporter);
			reducer = ReflectionUtils.newInstance(
					conf.getIterativeReducerClass(), conf);
		}

		public RecordReader<OUTKEY, OUTVALUE> getTermCheckReader() {
			return reader;
		}

		public float accumulateDistance(OUTKEY key, OUTVALUE oldvalue,
				OUTVALUE newvalue) throws IOException {
			float difference = reducer.distance(key, oldvalue, newvalue);
			distance += difference;
			return difference;
		}

		public float getSingleDistance(OUTKEY key, OUTVALUE oldvalue,
				OUTVALUE newvalue) throws IOException {
			return reducer.distance(key, oldvalue, newvalue);
		}

		public float getDistance() {
			return distance;
		}

		private RecordReader<OUTKEY, OUTVALUE> getLastResultRecordReader(
				int currentIteration, Reporter reporter) throws IOException {
			FileSystem lfs = FileSystem.getLocal(conf);
			Path lastresult;
			if(currentIteration == 1){
				lastresult = new Path(conf.get("mapred.incr.tmp", "/mnt/iteroop/")
						+ conf.getIterativeAlgorithmID() + "/filter-0." 
						+ getTaskID().getTaskID().getId());
			}else{
				lastresult = new Path(conf.get("mapred.incr.tmp", "/mnt/iteroop/")
						+ conf.getIterativeAlgorithmID() + "/substate-"
						+ (currentIteration - 1) + "."
						+ getTaskID().getTaskID().getId());
			}
			long filelen = lfs.getFileStatus(lastresult).getLen();
			InputSplit inputSplit = new FileSplit(lastresult, 0, filelen, null,
					true);
			return conf.getResultInputFormat().getRecordReader(inputSplit,
					conf, reporter);
		}

		public void close() throws IOException {
			reader.close();
		}
	}

	public class ResultFile<OUTKEY, OUTVALUE> {
		RecordReader<OUTKEY, OUTVALUE> reader;
		OUTKEY key;
		OUTVALUE value;
		int priority;

		public ResultFile(RecordReader<OUTKEY, OUTVALUE> inreader, int priority) {
			this.priority = priority;
			reader = inreader;
			key = reader.createKey();
			value = reader.createValue();
			try {
				reader.next(key, value);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public OUTKEY getKey() {
			return key;
		}

		public OUTVALUE getValue() {
			return value;
		}

		public int getPriority() {
			return priority;
		}

		public boolean next() throws IOException {
			return reader.next(key, value);
		}

		public void close() throws IOException {
			reader.close();
		}
	}

	private class ResultFileQueue<INKEY, INVALUE, OUTKEY, OUTVALUE> extends
			PriorityQueue<ResultFile<OUTKEY, OUTVALUE>> {

		// List<ResultFile<OUTKEY,OUTVALUE>> rfile = new
		// ArrayList<ResultFile<OUTKEY,OUTVALUE>>();
		ResultFile<OUTKEY, OUTVALUE> minRFile;
		RawComparator<OUTKEY> comparator;
		int taskid;
		OUTKEY key;
		OUTVALUE value;

		public ResultFileQueue(int currIteration, Reporter reporter)
				throws IOException {
			taskid = getTaskID().getTaskID().getId();
			comparator = conf.getOutputKeyComparator();

			/*
			 * //for multiple preserve files if(conf.isIncrementalStart()){
			 * initialize(1); }else if(conf.isIncrementalIterative()){
			 * initialize(conf.getIterationNum() + 1); }
			 */

			// for single preserve file
			initialize(1);

			addResultFiles(currIteration, reporter);
		}

		public void close() throws IOException {
			ResultFile<OUTKEY, OUTVALUE> result;
			while ((result = pop()) != null) {
				result.close();
			}
		}

		public OUTKEY getKey() {
			return key;
		}

		public OUTVALUE getValue() {
			return value;
		}

		public int getPriority() {
			return minRFile.getPriority();
		}

		private void adjustPriorityQueue(ResultFile<OUTKEY, OUTVALUE> reader)
				throws IOException {
			boolean hasNext = reader.next();
			if (hasNext) {
				adjustTop();
			} else {
				pop();
				reader.close();
			}
		}

		public boolean next() throws IOException {
			// LOG.info("the size is " + size());
			if (size() == 0)
				return false;

			if (minRFile != null) {
				// minSegment is non-null for all invocations of next except the
				// first
				// one. For the first invocation, the priority queue is ready
				// for use
				// but for the subsequent invocations, first adjust the queue
				adjustPriorityQueue(minRFile);
				if (size() == 0) {
					minRFile = null;
					return false;
				}
			}
			minRFile = top();

			key = minRFile.getKey();
			value = minRFile.getValue();

			return true;
		}

		@SuppressWarnings("unchecked")
		protected boolean lessThan(Object a, Object b) {
			OUTKEY key1 = ((ResultFile<OUTKEY, OUTVALUE>) a).getKey();
			OUTKEY key2 = ((ResultFile<OUTKEY, OUTVALUE>) b).getKey();

			return comparator.compare(key1, key2) < 0;
		}

		// for multiple preserve file
		private void addResultFiles2(int currentIteration, Reporter reporter)
				throws IOException {
			/*
			 * rfile.add(new ResultFile(getOldPrevResultReader(conf, reporter),
			 * 0)); for(int i=1; i<currentIteration; i++){ rfile.add(new
			 * ResultFile(getIncrementalPrevResultReader(conf, i, reporter),
			 * i)); }
			 */
			put(new ResultFile(getOldPrevResultReader(conf, reporter), 0));
			for (int i = 1; i <= currentIteration; i++) {
				put(new ResultFile(getIncrementalPrevResultReader(conf, i,
						reporter), i));
			}
		}

		// for single preserve file
		private void addResultFiles(int currentIteration, Reporter reporter)
				throws IOException {
			/*
			 * rfile.add(new ResultFile(getOldPrevResultReader(conf, reporter),
			 * 0)); for(int i=1; i<currentIteration; i++){ rfile.add(new
			 * ResultFile(getIncrementalPrevResultReader(conf, i, reporter),
			 * i)); }
			 */
			if (currentIteration == -1) {
				put(new ResultFile(getOldPrevResultReader(conf, reporter), 0));
			} else {
				put(new ResultFile(getIncrementalPrevResultReader(conf,
						(currentIteration - 1), reporter), 0));
			}
		}

		private RecordReader<OUTKEY, OUTVALUE> getOldPrevResultReader(
				JobConf job, Reporter reporter) throws IOException {
			if (job.getDynamicDataPath() == null)
				throw new IOException("we need the converged result data "
						+ "to perform incremental computation!!!");

			// download the remote old static data
			Path remotePrevConvPath = new Path(job.getDynamicDataPath() + "/"
					+ getOutputName(taskid));
			Path localPrevConvPath = new Path(job.get("mapred.incr.tmp", "/mnt/iteroop/")
					+ job.getIterativeAlgorithmID() + "/prevconv-" + taskid);

			if (hdfs.exists(remotePrevConvPath)) {
				// if it doesn't exist the local static file, it means it is the
				// first iteration, so copy it from hdfs
				if (!localfs.exists(localPrevConvPath)) {
					hdfs.copyToLocalFile(remotePrevConvPath, localPrevConvPath);
					LOG.info("copy remote old converged file "
							+ remotePrevConvPath + " to local disk"
							+ localPrevConvPath + "!!!!!!!!!");
				}

				// load static data
				long filelen = localfs.getFileStatus(localPrevConvPath)
						.getLen();
				InputSplit inputSplit = new FileSplit(localPrevConvPath, 0,
						filelen, null, true);
				return conf.getResultInputFormat().getRecordReader(inputSplit,
						conf, reporter);
			} else {
				throw new IOException(
						"acturally, there is no previous converged result on the path you"
								+ " have set! Please check the path and check the map task number "
								+ remotePrevConvPath);
			}
		}

		private RecordReader<OUTKEY, OUTVALUE> getIncrementalPrevResultReader(
				JobConf job, int iteration, Reporter reporter)
				throws IOException {

			// download the remote old static data
			Path remotePrevConvPath = new Path(job.getDynamicDataPath() + "/"
					+ getOutputName(getPartition()));
			Path localPrevConvPath = new Path(job.get("mapred.incr.tmp", "/mnt/iteroop/")
					+ job.getIterativeAlgorithmID() + "/substate-" + iteration
					+ "." + taskid);

			if (!localfs.exists(localPrevConvPath)) {
				if (hdfs.exists(remotePrevConvPath)) {
					hdfs.copyToLocalFile(remotePrevConvPath, localPrevConvPath);
					LOG.info("copy remote incremental previous result file "
							+ remotePrevConvPath + " to local disk"
							+ localPrevConvPath + "!!!!!!!!!");
				} else {
					throw new IOException(
							"acturally, there is no previous converged result on the path you"
									+ " have set! Please check the path and check the map task number "
									+ remotePrevConvPath);
				}
			}

			// load static data
			long filelen = localfs.getFileStatus(localPrevConvPath).getLen();
			InputSplit inputSplit = new FileSplit(localPrevConvPath, 0,
					filelen, null, true);
			return conf.getResultInputFormat().getRecordReader(inputSplit,
					conf, reporter);
		}
	}

	@SuppressWarnings("unchecked")
	private <INKEY, INVALUE, OUTKEY, OUTVALUE> void runOldReducer(JobConf job,
			TaskUmbilicalProtocol umbilical, final TaskReporter reporter,
			RawKeyValueIterator rIter, RawComparator<INKEY> comparator,
			Class<INKEY> keyClass, Class<INVALUE> valueClass)
			throws IOException {
		Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE> reducer = ReflectionUtils
				.newInstance(job.getReducerClass(), job);
		// make output collector
		String finalName = getOutputName(getPartition());

		final RecordWriter<OUTKEY, OUTVALUE> out = new OldTrackingRecordWriter<OUTKEY, OUTVALUE>(
				reduceOutputCounter, job, reporter, finalName, true);

		OutputCollector<OUTKEY, OUTVALUE> collector = new OutputCollector<OUTKEY, OUTVALUE>() {
			public void collect(OUTKEY key, OUTVALUE value) throws IOException {
				out.write(key, value);
				// indicate that progress update needs to be sent
				reporter.progress();
			}
		};

		// apply reduce function
		try {
			// increment processed counter only if skipping feature is enabled
			boolean incrProcCount = SkipBadRecords.getReducerMaxSkipGroups(job) > 0
					&& SkipBadRecords.getAutoIncrReducerProcCount(job);

			ReduceValuesIterator<INKEY, INVALUE> values = isSkipping() ? new SkippingReduceValuesIterator<INKEY, INVALUE>(
					rIter, comparator, keyClass, valueClass, job, reporter,
					umbilical) : new ReduceValuesIterator<INKEY, INVALUE>(
					rIter, job.getOutputValueGroupingComparator(), keyClass,
					valueClass, job, reporter);
			values.informReduceProgress();
			while (values.more()) {
				reduceInputKeyCounter.increment(1);

				reducer.reduce(values.getKey(), values, collector, reporter);

				if (incrProcCount) {
					reporter.incrCounter(SkipBadRecords.COUNTER_GROUP,
							SkipBadRecords.COUNTER_REDUCE_PROCESSED_GROUPS, 1);
				}
				values.nextKey();
				values.informReduceProgress();
			}

			// Clean up: repeated in catch block below
			reducer.close();
			out.close(reporter);

			// End of clean up.
		} catch (IOException ioe) {
			try {
				reducer.close();
			} catch (IOException ignored) {
			}

			try {
				out.close(reporter);
			} catch (IOException ignored) {
			}

			throw ioe;
		}
	}

	@SuppressWarnings("unchecked")
	private <INKEY, INVALUE, OUTKEY, OUTVALUE> void runBatchPreserveReducer(
			JobConf job, TaskUmbilicalProtocol umbilical,
			final TaskReporter reporter, RawKeyValueIterator rIter,
			RawComparator<INKEY> comparator, Class<INKEY> keyClass,
			Class<INVALUE> valueClass) throws IOException {
		Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE> reducer = ReflectionUtils
				.newInstance(job.getReducerClass(), job);
		String finalName = getOutputName(getPartition());

		final RecordWriter<OUTKEY, OUTVALUE> out = new OldTrackingRecordWriter<OUTKEY, OUTVALUE>(
				reduceOutputCounter, job, reporter, finalName, true);
		OutputCollector<OUTKEY, OUTVALUE> collector;
		String tmp = "/tmp/easy/" + System.currentTimeMillis();
		Class c1 = job.getOutputKeyClass();
		Class c2 = job.getOutputValueClass();
		int taskid = this.getTaskID().getTaskID().getId();
		final BatchPreserveFile<OUTKEY, OUTVALUE> file = new BatchPreserveFile<OUTKEY, OUTVALUE>(
				job, new Path(tmp + "/preserve-" + taskid), c1, c2);
		collector = new OutputCollector<OUTKEY, OUTVALUE>() {
			public void collect(OUTKEY key, OUTVALUE value) throws IOException {
				out.write(key, value);
				OUTKEY k = key;
				OUTVALUE v = value;
				file.put(k, v);
				reporter.progress();
			}
		};
		try {
			// increment processed counter only if skipping feature is enabled
			boolean incrProcCount = SkipBadRecords.getReducerMaxSkipGroups(job) > 0
					&& SkipBadRecords.getAutoIncrReducerProcCount(job);

			ReduceValuesIterator<INKEY, INVALUE> values = isSkipping() ? new SkippingReduceValuesIterator<INKEY, INVALUE>(
					rIter, comparator, keyClass, valueClass, job, reporter,
					umbilical) : new ReduceValuesIterator<INKEY, INVALUE>(
					rIter, job.getOutputValueGroupingComparator(), keyClass,
					valueClass, job, reporter);
			values.informReduceProgress();
			while (values.more()) {
				reduceInputKeyCounter.increment(1);

				reducer.reduce(values.getKey(), values, collector, reporter);

				if (incrProcCount) {
					reporter.incrCounter(SkipBadRecords.COUNTER_GROUP,
							SkipBadRecords.COUNTER_REDUCE_PROCESSED_GROUPS, 1);
				}
				values.nextKey();
				values.informReduceProgress();
			}

			reducer.close();
			out.close(reporter);
			file.close();
			FileSystem hdfs = FileSystem.get(job);
			this.localfs
					.delete(new Path(tmp + "/.preserve-" + taskid + ".crc"));
			hdfs.copyFromLocalFile(false,
					new Path(tmp + "/preserve-" + taskid),
					new Path(job.get("mapred.output.dir")
							+ "-preserve/preserve-" + taskid));

			// End of clean up.
		} catch (IOException ioe) {
			try {
				reducer.close();
			} catch (IOException ignored) {
			}

			try {
				out.close(reporter);
			} catch (IOException ignored) {
			}

			throw ioe;
		}

	}

	@SuppressWarnings("unchecked")
	private <INKEY, INVALUE, OUTKEY, OUTVALUE> void runBatchIncrementalReducer(
			JobConf job, TaskUmbilicalProtocol umbilical,
			final TaskReporter reporter, RawKeyValueIterator rIter,
			RawComparator<INKEY> comparator, Class<INKEY> keyClass,
			Class<INVALUE> valueClass) throws IOException {
		int taskid = this.getTaskID().getTaskID().getId();
		String remotePreservedPath = job.getPreserveStatePath();
		String localPreservedPath = "/tmp/easy/incr"
				+ System.currentTimeMillis();
		if (hdfs.exists(new Path(remotePreservedPath))) {
			hdfs.copyToLocalFile(new Path(remotePreservedPath + "/preserve-"
					+ taskid), new Path(localPreservedPath + "/preserve-"
					+ taskid));
			LOG.info("copy remote preserve file "
					+ remotePreservedPath
					+ " to local disk"
					+ localPreservedPath
					+ "!!!!!!!!! and file size is "
					+ localfs
							.getFileStatus(
									new Path(localPreservedPath + "/preserve-"
											+ taskid)).getLen());
		}
		Class c1 = job.getOutputKeyClass();
		Class c2 = job.getOutputValueClass();
		final BatchPreserveFile<OUTKEY, OUTVALUE> preserveFile = new BatchPreserveFile<OUTKEY, OUTVALUE>(
				job, new Path(localPreservedPath + "/preserve-" + taskid), c1,
				c2);

		final EasyReducer<INKEY, INVALUE, OUTKEY, OUTVALUE> reducer;
		reducer = ReflectionUtils.newInstance(job.getEasyReducerClass(), job);
		// make output collector
		String finalName = getOutputName(getPartition());

		final RecordWriter<OUTKEY, OUTVALUE> out = new OldTrackingRecordWriter<OUTKEY, OUTVALUE>(
				reduceOutputCounter, job, reporter, finalName, true);

		OutputCollector<OUTKEY, OUTVALUE> collector = new OutputCollector<OUTKEY, OUTVALUE>() {
			public void collect(OUTKEY key, OUTVALUE value) throws IOException {
				OUTVALUE newval = reducer.incremental(preserveFile.get(key),
						value);
				if (newval != null) {
					out.write(key, newval);
					preserveFile.put(key, newval);
				}
				// indicate that progress update needs to be sent
				reporter.progress();
			}
		};

		// apply reduce function
		try {
			// increment processed counter only if skipping feature is enabled
			boolean incrProcCount = SkipBadRecords.getReducerMaxSkipGroups(job) > 0
					&& SkipBadRecords.getAutoIncrReducerProcCount(job);

			ReduceValuesIterator<INKEY, INVALUE> values = isSkipping() ? new SkippingReduceValuesIterator<INKEY, INVALUE>(
					rIter, comparator, keyClass, valueClass, job, reporter,
					umbilical) : new ReduceValuesIterator<INKEY, INVALUE>(
					rIter, job.getOutputValueGroupingComparator(), keyClass,
					valueClass, job, reporter);
			values.informReduceProgress();
			while (values.more()) {
				reduceInputKeyCounter.increment(1);
				reducer.reduce(values.getKey(), values, collector, reporter);

				if (incrProcCount) {
					reporter.incrCounter(SkipBadRecords.COUNTER_GROUP,
							SkipBadRecords.COUNTER_REDUCE_PROCESSED_GROUPS, 1);
				}
				values.nextKey();
				values.informReduceProgress();
			}

			// Clean up: repeated in catch block below
			reducer.close();
			out.close(reporter);
			preserveFile.close();
			FileSystem hdfs = FileSystem.get(job);
			this.localfs.delete(new Path(localPreservedPath + "/.preserve-"
					+ taskid + ".crc"));
			hdfs.copyFromLocalFile(false, new Path(localPreservedPath
					+ "/preserve-" + taskid),
					new Path(job.get("mapred.output.dir")
							+ "-preserve/preserve-" + taskid));
			// End of clean up.
		} catch (IOException ioe) {
			try {
				reducer.close();
			} catch (IOException ignored) {
			}

			try {
				out.close(reporter);
			} catch (IOException ignored) {
			}

			throw ioe;
		}

	}

	@SuppressWarnings("unchecked")
	private <INKEY, INVALUE, OUTKEY, OUTVALUE> void runIterativeReducer(
			JobConf job, TaskUmbilicalProtocol umbilical,
			final TaskReporter reporter, int iteration,
			RawKeyValueIterator rIter, RawComparator<INKEY> comparator,
			Class<INKEY> keyClass, Class<INVALUE> valueClass)
			throws IOException {

		long taskstart = new Date().getTime();

		IterativeReducer<INKEY, INVALUE, OUTKEY, OUTVALUE> reducer = ReflectionUtils
				.newInstance(job.getIterativeReducerClass(), job);

		boolean writeHDFS = false;
		if (iteration % job.getCheckPointInterval() == 0
				|| job.getCheckPointInterval() == -1) {
			writeHDFS = true;
		}
		if (job.getOutputKeyClass().equals(GlobalUniqKeyWritable.class))
			writeHDFS = false;

		// result is dump to local fs, reduce output is localkvstate, create
		// local state data files on local fs
		String localfilename = new String(job.get("mapred.incr.tmp", "/mnt/iteroop/")
				+ job.getIterativeAlgorithmID() + "/substate-" + iteration
				+ "." + this.getTaskID().getTaskID().getId());
		LOG.info("the local output file is " + localfilename);
		final RecordWriter<OUTKEY, OUTVALUE> localOut = new OldTrackingRecordWriter<OUTKEY, OUTVALUE>(
				reduceOutputCounter, job, reporter, localfilename, false);

		// whether to perform termination check based on distance
		float threshold = job.getDistanceThreshold();
		TerminateChecker<INKEY, INVALUE, OUTKEY, OUTVALUE> termchecker = null;
		boolean termcheck = ((threshold == -1) ? false : true)
				&& (iteration > 1); // perform term check from the second
									// iteration
		if (termcheck) {
			termchecker = new TerminateChecker<INKEY, INVALUE, OUTKEY, OUTVALUE>(
					iteration, reporter);
		}

		OutputCollector<OUTKEY, OUTVALUE> collector;
		collector = new WrappedOutputLocalCollector<INKEY, INVALUE, OUTKEY, OUTVALUE>(
				localOut, reporter, termchecker);

		// apply reduce function
		try {
			// increment processed counter only if skipping feature is enabled
			boolean incrProcCount = SkipBadRecords.getReducerMaxSkipGroups(job) > 0
					&& SkipBadRecords.getAutoIncrReducerProcCount(job);

			ReduceValuesIterator<INKEY, INVALUE> values = isSkipping() ? new SkippingReduceValuesIterator<INKEY, INVALUE>(
					rIter, comparator, keyClass, valueClass, job, reporter,
					umbilical) : new ReduceValuesIterator<INKEY, INVALUE>(
					rIter, job.getOutputValueGroupingComparator(), keyClass,
					valueClass, job, reporter);
			values.informReduceProgress();

			while (values.more()) {
				reduceInputKeyCounter.increment(1);

				reducer.reduce(values.getKey(), values, collector, reporter);

				if (incrProcCount) {
					reporter.incrCounter(SkipBadRecords.COUNTER_GROUP,
							SkipBadRecords.COUNTER_REDUCE_PROCESSED_GROUPS, 1);
				}
				values.nextKey();
				values.informReduceProgress();
			}

			LOG.info("created file length "
					+ localfs.getFileStatus(new Path(localfilename)).getLen());

			// Clean up: repeated in catch block below
			reducer.close();
			localOut.close(reporter);
			if (termcheck)
				termchecker.close();

			if (writeHDFS) {
				FileSystem hdfs = FileSystem.get(job);
				hdfs.copyFromLocalFile(false, new Path(localfilename),
						new Path(job.get("mapred.output.dir") + "/iteration-"
								+ iteration + "/"
								+ getOutputName(getPartition())));
			}

			// if global data, need to send to jobtracker, then the jobtracker
			// will combine them
			if (job.getGlobalUniqValuePath() != null) {
				LOG.info("send global data to jobtracker!");
				sendGlobalData(job, localfilename, iteration, reporter);
			}

			// End of clean up.
		} catch (IOException ioe) {
			try {
				reducer.close();
			} catch (IOException ignored) {
			}

			try {
				localOut.close(reporter);
				if (termcheck)
					termchecker.close();
			} catch (IOException ignored) {
			}

			throw ioe;
		}

		long taskend = new Date().getTime();

		LOG.info("iteration " + iteration + " takes " + (taskend - taskstart));

		// report iterative task information
		IterativeTaskCompletionEvent event = new IterativeTaskCompletionEvent(
				getJobID(), iteration, this.getTaskID(), this.getTaskID()
						.getTaskID().getId(), this.isMapTask());
		event.setProcessedRecords(reduceInputKeyCounter.getCounter());
		event.setRunTime(taskend - taskstart);
		if (termcheck) {
			event.setSubDistance(termchecker.getDistance());
		}

		try {
			umbilical.iterativeTaskComplete(event);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		reducer.iteration_complete(iteration);
	}

	@SuppressWarnings("unchecked")
	private <INKEY, INVALUE, OUTKEY, OUTVALUE> void runIterCPCReducer(
			JobConf job, TaskUmbilicalProtocol umbilical,
			final TaskReporter reporter, int iteration,
			RawKeyValueIterator rIter, RawComparator<INKEY> comparator,
			Class<INKEY> keyClass, Class<INVALUE> valueClass)
			throws IOException {

		long taskstart = new Date().getTime();

		IterativeReducer<INKEY, INVALUE, OUTKEY, OUTVALUE> reducer = ReflectionUtils
				.newInstance(job.getIterativeReducerClass(), job);

		boolean writeHDFS = false;
		if (iteration % job.getCheckPointInterval() == 0
				|| job.getCheckPointInterval() == -1) {
			writeHDFS = true;
		}
		if (job.getOutputKeyClass().equals(GlobalUniqKeyWritable.class))
			writeHDFS = false;

		// result is dump to local fs, reduce output is localkvstate, create
		// local state data files on local fs
		String localfilename = new String(job.get("mapred.incr.tmp", "/mnt/iteroop/")
				+ job.getIterativeAlgorithmID() + "/substate-" + iteration
				+ "." + this.getTaskID().getTaskID().getId());
		LOG.info("the local output file is " + localfilename);
		final RecordWriter<OUTKEY, OUTVALUE> localOut = new OldTrackingRecordWriter<OUTKEY, OUTVALUE>(
				reduceOutputCounter, job, reporter, localfilename, false);	
		
		String filteredfilename = new String(job.get("mapred.incr.tmp", "/mnt/iteroop/")
				+ job.getIterativeAlgorithmID() + "/filter-" + iteration + "."
				+ this.getTaskID().getTaskID().getId());
		final RecordWriter<OUTKEY, OUTVALUE> filteredOut = new OldTrackingRecordWriter<OUTKEY, OUTVALUE>(
				reduceOutputCounter, job, reporter, filteredfilename, false);
		
		//create last filter out reader
		Path lastresult = new Path(conf.get("mapred.incr.tmp", "/mnt/iteroop/")
				+ conf.getIterativeAlgorithmID() + "/filter-"
				+ (iteration - 1) + "."
				+ getTaskID().getTaskID().getId());
		FileSystem lfs = FileSystem.getLocal(conf);
		long filelen = lfs.getFileStatus(lastresult).getLen();
		InputSplit inputSplit = new FileSplit(lastresult, 0, filelen, null, true);
		RecordReader<OUTKEY, OUTVALUE> lastResultReader = 
				conf.getResultInputFormat().getRecordReader(inputSplit,	conf, reporter);
	
		// use TermChecker to perform filter
		TerminateChecker<INKEY, INVALUE, OUTKEY, OUTVALUE> termchecker = 
				new TerminateChecker<INKEY, INVALUE, OUTKEY, OUTVALUE>(iteration, reporter);
		
		float filter_threshold = job.getFilterThreshold();
		WrappedFilteredOutputLocalCollector<INKEY, INVALUE, OUTKEY, OUTVALUE> collector = 
				new WrappedFilteredOutputLocalCollector<INKEY, INVALUE, OUTKEY, OUTVALUE>(
				localOut, filteredOut, reporter, termchecker, lastResultReader, 
				filter_threshold, comparator, iteration);

		// apply reduce function
		try {
			// increment processed counter only if skipping feature is enabled
			boolean incrProcCount = SkipBadRecords.getReducerMaxSkipGroups(job) > 0
					&& SkipBadRecords.getAutoIncrReducerProcCount(job);

			ReduceValuesIterator<INKEY, INVALUE> values = isSkipping() ? new SkippingReduceValuesIterator<INKEY, INVALUE>(
					rIter, comparator, keyClass, valueClass, job, reporter,
					umbilical) : new ReduceValuesIterator<INKEY, INVALUE>(
					rIter, job.getOutputValueGroupingComparator(), keyClass,
					valueClass, job, reporter);
			values.informReduceProgress();

			while (values.more()) {
				reduceInputKeyCounter.increment(1);

				reducer.reduce(values.getKey(), values, collector, reporter);

				if (incrProcCount) {
					reporter.incrCounter(SkipBadRecords.COUNTER_GROUP,
							SkipBadRecords.COUNTER_REDUCE_PROCESSED_GROUPS, 1);
				}
				values.nextKey();
				values.informReduceProgress();
			}

			LOG.info("created file length "
					+ localfs.getFileStatus(new Path(localfilename)).getLen());

			// Clean up: repeated in catch block below
			reducer.close();
			localOut.close(reporter);
			filteredOut.close(reporter);
			termchecker.close();

			if (writeHDFS) {
				FileSystem hdfs = FileSystem.get(job);
				hdfs.copyFromLocalFile(false, new Path(localfilename),
						new Path(job.get("mapred.output.dir") + "/iteration-"
								+ iteration + "/"
								+ getOutputName(getPartition())));
			}

			// if global data, need to send to jobtracker, then the jobtracker
			// will combine them
			if (job.getGlobalUniqValuePath() != null) {
				LOG.info("send global data to jobtracker!");
				sendGlobalData(job, localfilename, iteration, reporter);
			}

			// End of clean up.
		} catch (IOException ioe) {
			try {
				reducer.close();
			} catch (IOException ignored) {
			}

			try {
				localOut.close(reporter);
				termchecker.close();
			} catch (IOException ignored) {
			}

			throw ioe;
		}

		long taskend = new Date().getTime();

		LOG.info("iteration " + iteration + " takes " + (taskend - taskstart));

		// report iterative task information
		IterativeTaskCompletionEvent event = new IterativeTaskCompletionEvent(
				getJobID(), iteration, this.getTaskID(), this.getTaskID()
						.getTaskID().getId(), this.isMapTask());
		event.setProcessedRecords(reduceInputKeyCounter.getCounter());
		event.setRunTime(taskend - taskstart);
		
		LOG.info("iteration " + iteration + " non converged items number is "
				+ ((WrappedFilteredOutputLocalCollector)collector).getNonConvNum());

		try {
			umbilical.iterativeTaskComplete(event);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		reducer.iteration_complete(iteration);
	}
	
	@SuppressWarnings("unchecked")
	private <INKEY, INVALUE, OUTKEY, OUTVALUE, SOURCEKEY> void runPreserveReducer(
			JobConf job, TaskUmbilicalProtocol umbilical,
			final TaskReporter reporter, RawKeyValueSourceIterator rIter,
			RawComparator<INKEY> comparator, Class<INKEY> keyClass,
			Class<INVALUE> valueClass, Class<SOURCEKEY> skeyClass,
			Class<OUTVALUE> outvalueClass) throws IOException {

		long taskstart = new Date().getTime();

		IterativeReducer<INKEY, INVALUE, OUTKEY, OUTVALUE> reducer = ReflectionUtils
				.newInstance(job.getIterativeReducerClass(), job);

		/*
		 * boolean writeHDFS = false; if(job.getIterationNum() %
		 * job.getCheckPointInterval() == 0 || job.getCheckPointInterval() ==
		 * -1){ writeHDFS = true; }
		 * if(job.getOutputKeyClass().equals(GlobalUniqKeyWritable.class))
		 * writeHDFS = false;
		 */

		// result is dump to local fs, reduce output is localkvstate, create
		// local state data files on local fs
		String localfilename = new String(job.get("mapred.incr.tmp", "/mnt/iteroop/")
				+ job.getIterativeAlgorithmID() + "/substate-0."
				+ this.getTaskID().getTaskID().getId());
		final RecordWriter<INKEY, OUTVALUE> localOut = new OldTrackingRecordWriter<INKEY, OUTVALUE>(
				reduceOutputCounter, job, reporter, localfilename, false);

		/*
		 * //whether to perform termination check based on distance float
		 * threshold = job.getDistanceThreshold();
		 * TerminateChecker<INKEY,INVALUE,OUTKEY,OUTVALUE> termchecker = null;
		 * boolean termcheck = ((threshold == -1) ? false : true) &&
		 * (job.getIterationNum() > 1); //perform term check from the second
		 * iteration if(termcheck){ termchecker = new
		 * TerminateChecker<INKEY,INVALUE
		 * ,OUTKEY,OUTVALUE>(conf.getIterationNum(), reporter); }
		 */

		WrappedIndexOutputLocalCollector<INKEY, INVALUE, OUTKEY, OUTVALUE> collector = new WrappedIndexOutputLocalCollector<INKEY, INVALUE, OUTKEY, OUTVALUE>(
				reporter, null); // now, we skip terminator

		int taskid = this.getTaskID().getTaskID().getId();
		Path preservePath = new Path(job.get("mapred.incr.tmp", "/mnt/iteroop/")
				+ job.getIterativeAlgorithmID() + "/preserve-" + taskid);
		Path preserveIndexPath = new Path(job.get("mapred.incr.tmp", "/mnt/iteroop/")
				+ job.getIterativeAlgorithmID() + "/preserve-" + taskid
				+ ".index");

		// IFile.PreserveFile<INKEY, INVALUE, SOURCEKEY, OUTVALUE> writer =new
		// IFile.PreserveFile<INKEY, INVALUE, SOURCEKEY, OUTVALUE>
		// (job, preservePath, null, preserveIndexPath,-1,keyClass, valueClass,
		// skeyClass, outvalueClass);
		IFile.PreserveFile<INKEY, INVALUE, SOURCEKEY, OUTVALUE> writer = new IFile.PreserveFile<INKEY, INVALUE, SOURCEKEY, OUTVALUE>(
				job, preservePath, null, preserveIndexPath, keyClass,
				valueClass, skeyClass, outvalueClass,
				job.getPreserveBufferType() + 10*job.getNumReduceTasks(),job.getPreserveBufferInterval() );
		
		PreserveReduceValuesIterator<INKEY, INVALUE, SOURCEKEY, OUTVALUE> values = new PreserveReduceValuesIterator<INKEY, INVALUE, SOURCEKEY, OUTVALUE>(
				rIter, job.getOutputValueGroupingComparator(), keyClass,
				valueClass, skeyClass, outvalueClass, writer, job, reporter);

		// apply reduce function
		try {
			// increment processed counter only if skipping feature is enabled
			boolean incrProcCount = SkipBadRecords.getReducerMaxSkipGroups(job) > 0
					&& SkipBadRecords.getAutoIncrReducerProcCount(job);

			if (isSkipping()) {
				throw new IOException("should consider skipping case!!!!");
			}
			values.informReduceProgress();

			while (values.more()) {
				reduceInputKeyCounter.increment(1);

				collector.clearCollectedValue();
				reducer.reduce(values.getKey(), values, collector, reporter);
				OUTVALUE value = collector.collectedValue();
				values.appendResKV(values.getKey(), value);
				localOut.write(values.getKey(), value);

				if (incrProcCount) {
					reporter.incrCounter(SkipBadRecords.COUNTER_GROUP,
							SkipBadRecords.COUNTER_REDUCE_PROCESSED_GROUPS, 1);
				}
				values.nextKey();
				values.informReduceProgress();
			}

			// Clean up: repeated in catch block below
			values.close(); // close the k,sk,v writer
			reducer.close();
			localOut.close(reporter);
			// if(termcheck) termchecker.close();

			boolean writeHDFS = true;
			if (writeHDFS) {
				FileSystem hdfs = FileSystem.get(job);
				hdfs.copyFromLocalFile(false, preservePath,
						new Path(job.getPreserveStatePath() + "/preserve-"
								+ taskid));
				hdfs.copyFromLocalFile(false, preserveIndexPath,
						new Path(job.getPreserveStatePath() + "/preserve-"
								+ taskid + ".index"));

				hdfs.copyFromLocalFile(false, new Path(localfilename),
						new Path(job.get("mapred.output.dir") + "/"
								+ getOutputName(getPartition())));
			}

			/*
			 * //if global data, need to send to jobtracker, then the jobtracker
			 * will combine them if(job.getGlobalUniqValuePath() != null) {
			 * LOG.info("send global data to jobtracker!"); sendGlobalData(job,
			 * localfilename, reporter); }
			 */

			// End of clean up.
		} catch (IOException ioe) {
			try {
				reducer.close();
			} catch (IOException ignored) {
			}

			try {
				writer.close();
				// if(termcheck) termchecker.close();
			} catch (IOException ignored) {
			}

			throw ioe;
		}

		long taskend = new Date().getTime();

		// report iterative task information
		IterativeTaskCompletionEvent event = new IterativeTaskCompletionEvent(
				getJobID(), 1, this.getTaskID(), this.getTaskID().getTaskID()
						.getId(), this.isMapTask());
		event.setProcessedRecords(reduceInputKeyCounter.getCounter());
		event.setRunTime(taskend - taskstart);
		/*
		 * if(termcheck){ event.setSubDistance(termchecker.getDistance()); }
		 */

		try {
			umbilical.iterativeTaskComplete(event);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void sendGlobalData(JobConf job, String localResult, int iteration,
			TaskReporter reporter) throws IOException {
		FileSystem localfs = FileSystem.getLocal(job);
		Path localresultpath = new Path(localResult);
		long filelen = localfs.getFileStatus(localresultpath).getLen();

		InputSplit inputSplit = new FileSplit(localresultpath, 0, filelen,
				null, true);

		// here, we use the result reader to read the local result
		RecordReader<GlobalUniqKeyWritable, GlobalUniqValueWritable> resultReader = job
				.getDynamicInputFormat().getRecordReader(inputSplit, job,
						reporter);

		GlobalUniqKeyWritable key = resultReader.createKey();
		GlobalUniqValueWritable value = resultReader.createValue();

		while (resultReader.next(key, value)) {
		}

		LOG.info("global value is " + value);

		GlobalData globaldata = new GlobalData(value, getJobID(), iteration);
		// globaldata.set(value);
		// globaldata.setConf(job);
		// LOG.info("global value is " + globaldata.get());

		try {
			umbilical.transmitGlobalData(globaldata);
		} catch (Exception e) {
			e.printStackTrace();
		}

		resultReader.close();
	}

	@SuppressWarnings("unchecked")
	private <INKEY, INVALUE, OUTKEY, OUTVALUE, SOURCEKEY> void runIncrementalReducer(
			JobConf job, TaskUmbilicalProtocol umbilical,
			final TaskReporter reporter, RawKeyValueSourceIterator rIter,
			RawComparator<INKEY> comparator, Class<INKEY> keyClass,
			Class<INVALUE> valueClass, Class<SOURCEKEY> skeyClass,
			Class<OUTVALUE> outvalueClass) throws IOException {

		long taskstart = new Date().getTime();

		IterativeReducer<INKEY, INVALUE, OUTKEY, OUTVALUE> reducer = ReflectionUtils
				.newInstance(job.getIterativeReducerClass(), job);

		/*
		 * boolean writeHDFS = true;
		 * if(job.getOutputKeyClass().equals(GlobalUniqKeyWritable.class))
		 * writeHDFS = false;
		 */

		WrappedIndexOutputLocalCollector<INKEY, INVALUE, OUTKEY, OUTVALUE> collector = new WrappedIndexOutputLocalCollector<INKEY, INVALUE, OUTKEY, OUTVALUE>(
				reporter, null); // now, we skip terminator

		int taskid = this.getTaskID().getTaskID().getId();
		String filteredfilename = new String(job.get("mapred.incr.tmp", "/mnt/iteroop/")
				+ job.getIterativeAlgorithmID() + "/filter-0."
				+ this.getTaskID().getTaskID().getId());
		final RecordWriter<OUTKEY, OUTVALUE> filteredOut = new OldTrackingRecordWriter<OUTKEY, OUTVALUE>(
				reduceOutputCounter, job, reporter, filteredfilename, false);

		Path remotePreservedPath = new Path(job.getPreserveStatePath()
				+ "/preserve-" + taskid);
		Path remotePreservedIndexPath = new Path(job.getPreserveStatePath()
				+ "/preserve-" + taskid + ".index");
		Path preservedPath = new Path(job.get("mapred.incr.tmp", "/mnt/iteroop/")
				+ job.getIterativeAlgorithmID() + "/preserve-" + taskid);
		Path oldPreservedIndexPath = new Path(job.get("mapred.incr.tmp", "/mnt/iteroop/")
				+ job.getIterativeAlgorithmID() + "/preserve-" + taskid
				+ ".index");
		Path newPreserveIndexPath = new Path(job.get("mapred.incr.tmp", "/mnt/iteroop/")
				+ job.getIterativeAlgorithmID() + "/preserve-Incr-0-" + taskid
				+ ".index");

		IFile.PreserveFile<INKEY, INVALUE, SOURCEKEY, OUTVALUE> preserveFile;
		if (hdfs.exists(remotePreservedPath)) {
			// if it doesn't exist the local static file, it means it is the
			// first iteration, so copy it from hdfs
			if (!localfs.exists(preservedPath)) {
				hdfs.copyToLocalFile(remotePreservedPath, preservedPath);
				hdfs.copyToLocalFile(remotePreservedIndexPath,
						oldPreservedIndexPath);
				LOG.info("copy remote preserve file " + remotePreservedPath
						+ " to local disk" + preservedPath
						+ "!!!!!!!!! and file size is "
						+ localfs.getFileStatus(preservedPath).getLen());
			}

			// preserveFile = new IFile.PreserveFile<INKEY, INVALUE, SOURCEKEY,
			// OUTVALUE>(conf, preservedPath,
			// oldPreservedIndexPath, newPreserveIndexPath, 0,keyClass,
			// valueClass, skeyClass, outvalueClass);
			preserveFile = new IFile.PreserveFile<INKEY, INVALUE, SOURCEKEY, OUTVALUE>(
					conf, preservedPath, oldPreservedIndexPath,
					newPreserveIndexPath, keyClass, valueClass, skeyClass,
					outvalueClass, job.getPreserveBufferType()+ 10*job.getNumReduceTasks(),job.getPreserveBufferInterval() );

		} else {
			throw new IOException(
					"acturally, there is no preserve data on the path you"
							+ " have set! Please check the path and check the map task number "
							+ remotePreservedPath);
		}

		// FilterOutputLocalCollector<INKEY,INVALUE,OUTKEY,OUTVALUE> collector =
		// new FilterOutputLocalCollector<INKEY,INVALUE,OUTKEY,OUTVALUE>(
		// preserveFile, filteredOut, reporter);

		int nonConvergedItems = 0;
		// apply reduce function
		try {
			// increment processed counter only if skipping feature is enabled
			boolean incrProcCount = SkipBadRecords.getReducerMaxSkipGroups(job) > 0
					&& SkipBadRecords.getAutoIncrReducerProcCount(job);

			if (isSkipping()) {
				throw new IOException("should consider skipping case!!!!");
			}
			/*
			 * ValuesInFileIterator<INKEY,INVALUE,SOURCEKEY,OUTVALUE> values =
			 * new ValuesInFileIterator<INKEY,INVALUE,SOURCEKEY,OUTVALUE>(rIter,
			 * 0, job.getOutputValueGroupingComparator(), keyClass, valueClass,
			 * skeyClass, outvalueClass, preserveFile,
			 * getTaskID().getTaskID().getId(), reducer.removeLable(), job,
			 * reporter);
			 */

			ValuesInMemIterator<INKEY, INVALUE, SOURCEKEY, OUTVALUE> values = new ValuesInMemIterator<INKEY, INVALUE, SOURCEKEY, OUTVALUE>(
					rIter, 0, job.getOutputValueGroupingComparator(), keyClass,
					valueClass, skeyClass, outvalueClass, preserveFile,
					getTaskID().getTaskID().getId(), reducer.removeLable(),
					job, reporter);

			reducePhase.set(rIter.getProgress().get()); // update progress
			reporter.progress();

			float filter_threshold = conf.getFilterThreshold();

			while (values.more()) {
				reduceInputKeyCounter.increment(1);

				collector.clearCollectedValue();
				INKEY key = values.getKey();

				reducer.reduce(key, values, collector, reporter);
				OUTVALUE oldvalue = values.getPreservedOutValue();
				OUTKEY newkey = collector.collectedKey();
				OUTVALUE newvalue = collector.collectedValue();

				// update the result key value pair
				values.updateResKV(key, newvalue);

				// filter the neglective record, write the filtered records to
				// filteredOut
				float diff = reducer.distance(newkey, oldvalue, newvalue);

				// LOG.info("compute diff " + oldvalue + "\t" + newvalue + "\t"
				// + diff);
				// LOG.info("for key " + key + " source file " + largestPri +
				// " diff between " + latestValue + " and " + value + " is " +
				// diff);

				if (diff >= filter_threshold) {
					// localwriter.write(key, value);
					filteredOut.write(newkey, newvalue);
					// LOG.info("collect " + key + "\t" + value + " diff " +
					// diff);
					nonConvergedItems++;
				} else {
					// LOG.info("skip " + key + "\t" + value + " diff " + diff);
				}

				if (incrProcCount) {
					reporter.incrCounter(SkipBadRecords.COUNTER_GROUP,
							SkipBadRecords.COUNTER_REDUCE_PROCESSED_GROUPS, 1);
				}

				values.nextKey();

				reducePhase.set(rIter.getProgress().get()); // update progress
				reporter.progress();
			}

			// for single preserve file
			// values.writeRest();
			// collector.writeRestKVs();

			// Clean up: repeated in catch block below
			// localOut.close(reporter);
			filteredOut.close(reporter);
			// resultSet.close();
			values.close(); // close the k,sk,v writer
			reducer.close();

			boolean writeHDFS = false;
			if (writeHDFS) {
				FileSystem hdfs = FileSystem.get(job);
				this.localfs.delete(new Path(job.get("mapred.incr.tmp", "/mnt/iteroop/")
						+ job.getIterativeAlgorithmID() + "/.preserve-"
						+ taskid + ".crc"));
				hdfs.copyFromLocalFile(false, preservedPath,
						remotePreservedPath);
				hdfs.copyFromLocalFile(false, newPreserveIndexPath,
						remotePreservedIndexPath);
			}

			hdfs.copyFromLocalFile(false, new Path(filteredfilename), new Path(
					job.get("mapred.output.dir") + "/"
							+ getOutputName(getPartition())));

			/*
			 * //if global data, need to send to jobtracker, then the jobtracker
			 * will combine them if(job.getGlobalUniqValuePath() != null) {
			 * LOG.info("send global data to jobtracker!"); sendGlobalData(job,
			 * localfilename, reporter); }
			 */

			// End of clean up.
		} catch (IOException ioe) {
			try {
				reducer.close();
			} catch (IOException ignored) {
			}

			try {
				// localOut.close(reporter);
				filteredOut.close(reporter);
			} catch (IOException ignored) {
			}

			throw ioe;
		}

		long taskend = new Date().getTime();

		// report iterative task information
		IterativeTaskCompletionEvent event = new IterativeTaskCompletionEvent(
				getJobID(), 1, this.getTaskID(), this.getTaskID().getTaskID()
						.getId(), this.isMapTask());
		event.setProcessedRecords(reduceInputKeyCounter.getCounter());
		event.setRunTime(taskend - taskstart);
		// int nonconvitems =
		// ((FilterOutputLocalCollector<INKEY,INVALUE,OUTKEY,OUTVALUE>)collector).getNonConvergedItems();
		LOG.info("non converged items number is " + nonConvergedItems);
		event.setSubDistance(nonConvergedItems);

		try {
			umbilical.iterativeTaskComplete(event);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	private <INKEY, INVALUE, OUTKEY, OUTVALUE, SOURCEKEY> void runIncrementalIterativeReducer(
			JobConf job, TaskUmbilicalProtocol umbilical,
			final TaskReporter reporter, int iteration,
			RawKeyValueSourceIterator rIter, RawComparator<INKEY> comparator,
			Class<INKEY> keyClass, Class<INVALUE> valueClass,
			Class<SOURCEKEY> skeyClass, Class<OUTVALUE> outvalueClass,
			long starttime) throws IOException {

		long taskstart = new Date().getTime();

		IterativeReducer<INKEY, INVALUE, OUTKEY, OUTVALUE> reducer = ReflectionUtils
				.newInstance(job.getIterativeReducerClass(), job);

		/*
		 * boolean writeHDFS = true;
		 * if(job.getOutputKeyClass().equals(GlobalUniqKeyWritable.class))
		 * writeHDFS = false;
		 */

		WrappedIndexOutputLocalCollector<INKEY, INVALUE, OUTKEY, OUTVALUE> collector = new WrappedIndexOutputLocalCollector<INKEY, INVALUE, OUTKEY, OUTVALUE>(
				reporter, null); // now, we skip terminator

		int taskid = this.getTaskID().getTaskID().getId();
		String filteredfilename = new String(job.get("mapred.incr.tmp", "/mnt/iteroop/")
				+ job.getIterativeAlgorithmID() + "/filter-" + iteration + "."
				+ this.getTaskID().getTaskID().getId());
		final RecordWriter<OUTKEY, OUTVALUE> filteredOut = new OldTrackingRecordWriter<OUTKEY, OUTVALUE>(
				reduceOutputCounter, job, reporter, filteredfilename, false);

		Path remotePreservedPath = new Path(job.getPreserveStatePath()
				+ "/preserve-" + taskid);
		Path remotePreservedIndexPath = new Path(job.getPreserveStatePath()
				+ "/preserve-" + taskid + ".index");
		Path preservedPath = new Path(job.get("mapred.incr.tmp", "/mnt/iteroop/")
				+ job.getIterativeAlgorithmID() + "/preserve-" + taskid);
		Path oldPreservedIndexPath = new Path(job.get("mapred.incr.tmp", "/mnt/iteroop/")
				+ job.getIterativeAlgorithmID() + "/preserve-Incr-"
				+ (iteration - 1) + "-" + taskid + ".index");
		Path newPreserveIndexPath = new Path(job.get("mapred.incr.tmp", "/mnt/iteroop/")
				+ job.getIterativeAlgorithmID() + "/preserve-Incr-" + iteration
				+ "-" + taskid + ".index");

		IFile.PreserveFile<INKEY, INVALUE, SOURCEKEY, OUTVALUE> preserveFile;
		if (hdfs.exists(remotePreservedPath)) {
			// if it doesn't exist the local static file, it means it is the
			// first iteration, so copy it from hdfs
			if (!localfs.exists(preservedPath)) {
				hdfs.copyToLocalFile(remotePreservedPath, preservedPath);
				hdfs.copyToLocalFile(remotePreservedIndexPath,
						oldPreservedIndexPath);
				LOG.info("copy remote preserve file " + remotePreservedPath
						+ " to local disk" + preservedPath
						+ "!!!!!!!!! and file size is "
						+ localfs.getFileStatus(preservedPath).getLen());
			}

			// preserveFile = new IFile.PreserveFile<INKEY, INVALUE, SOURCEKEY,
			// OUTVALUE>(conf, preservedPath,
			// oldPreservedIndexPath, newPreserveIndexPath,iteration, keyClass,
			// valueClass, skeyClass, outvalueClass);
			preserveFile = new IFile.PreserveFile<INKEY, INVALUE, SOURCEKEY, OUTVALUE>(
					conf, preservedPath, oldPreservedIndexPath,
					newPreserveIndexPath, keyClass, valueClass, skeyClass,
					outvalueClass, job.getPreserveBufferType() +  10*job.getNumReduceTasks(),job.getPreserveBufferInterval()  );
		} else { 
			throw new IOException(
					"acturally, there is no preserve data on the path you"
							+ " have set! Please check the path and check the map task number "
							+ remotePreservedPath);
		}

		int nonConvergedItems = 0;
		// apply reduce function
		try {
			// increment processed counter only if skipping feature is enabled
			boolean incrProcCount = SkipBadRecords.getReducerMaxSkipGroups(job) > 0
					&& SkipBadRecords.getAutoIncrReducerProcCount(job);

			if (isSkipping()) {
				throw new IOException("should consider skipping case!!!!");
			}

			IncrementalSourceValuesIterator<INKEY, INVALUE, SOURCEKEY, OUTVALUE> values;

			if (job.isBufferReduceKVs()) {
				// this is the in-memory version
				values = new ValuesInMemIterator<INKEY, INVALUE, SOURCEKEY, OUTVALUE>(
						rIter, 0, job.getOutputValueGroupingComparator(),
						keyClass, valueClass, skeyClass, outvalueClass,
						preserveFile, getTaskID().getTaskID().getId(),
						reducer.removeLable(), job, reporter);
			} else {
				// this is the in-file version
				values = new ValuesInFileIterator<INKEY, INVALUE, SOURCEKEY, OUTVALUE>(
						rIter, 0, job.getOutputValueGroupingComparator(),
						keyClass, valueClass, skeyClass, outvalueClass,
						preserveFile, getTaskID().getTaskID().getId(),
						reducer.removeLable(), job, reporter);
			}

			reducePhase.set(rIter.getProgress().get()); // update progress
			reporter.progress();

			float filter_threshold = conf.getFilterThreshold();

			long time1 = System.currentTimeMillis();
			int processed = 0;
			while (values.more()) {
				reduceInputKeyCounter.increment(1);

				collector.clearCollectedValue();
				INKEY key = values.getKey();
				reducer.reduce(key, values, collector, reporter);
				OUTVALUE oldvalue = values.getPreservedOutValue();
				OUTKEY newkey = collector.collectedKey();
				OUTVALUE newvalue = collector.collectedValue();

				// update the result key value pair
				values.updateResKV(key, newvalue);

				// filter the neglective record, write the filtered records to
				// filteredOut
				float diff = reducer.distance(newkey, oldvalue, newvalue);

				// LOG.info("for key " + key + " source file " + largestPri +
				// " diff between " + latestValue + " and " + value + " is " +
				// diff);

				if (diff >= filter_threshold) {
					// localwriter.write(key, value);
					filteredOut.write(newkey, newvalue);
					// LOG.info("collect " + key + "\t" + newvalue + " diff " +
					// diff);
					nonConvergedItems++;
				} else {
					// LOG.info("skip " + key + "\t" + newvalue + " diff " +
					// diff);
				}

				if (incrProcCount) {
					reporter.incrCounter(SkipBadRecords.COUNTER_GROUP,
							SkipBadRecords.COUNTER_REDUCE_PROCESSED_GROUPS, 1);
				}
				processed++;

				values.nextKey();

				reducePhase.set(rIter.getProgress().get()); // update progress
				reporter.progress();
			}
			long time2 = System.currentTimeMillis();

			LOG.info("iteration " + iteration + " iterating " + processed
					+ " key-value pairs takes " + (time2 - time1) + " total "
					+ (time2 - starttime));
			// for single preserve file
			// values.writeRest();
			// collector.writeRestKVs();

			/**
			 * merging the results and obtain result file with unique keys
			 */
			 boolean writeHDFS = (job.getMaxIterations() == iteration);
//			boolean writeHDFS = false;

			if (writeHDFS) { 
				String resultFile = new String(job.get("mapred.incr.tmp", "/mnt/iteroop/")
						+ job.getIterativeAlgorithmID() + "/result-"
						+ iteration + "."
						+ this.getTaskID().getTaskID().getId());
				final RecordWriter<OUTKEY, OUTVALUE> resultOut = new OldTrackingRecordWriter<OUTKEY, OUTVALUE>(
						reduceOutputCounter, job, reporter, resultFile, false);
				preserveFile.writeResult(resultOut, job);
				filteredOut.close(reporter);
				values.close(); // close the k,sk,v writer
				reducer.close();

				resultOut.close(reporter);
				// dataFile.close();
				this.localfs.delete(new Path(job.get("mapred.incr.tmp", "/mnt/iteroop/")
						+ job.getIterativeAlgorithmID() + "/.result-"
						+ iteration + "."
						+ this.getTaskID().getTaskID().getId() + ".crc"));
				hdfs.copyFromLocalFile(false, new Path(resultFile), new Path(
						job.get("mapred.output.dir") + "/result/iteration-"
								+ iteration + "/" + getOutputName(getPartition())));
			} else {
				filteredOut.close(reporter);
				values.close(); // close the k,sk,v writer
				reducer.close();
			}
			/*
			 * boolean writeHDFS = (job.getMaxIterations() == iteration);
			 * if(writeHDFS){ hdfs.copyFromLocalFile(false, preservedPath,
			 * remotePreservedPath); hdfs.copyFromLocalFile(false,
			 * newPreserveIndexPath, remotePreservedIndexPath); }
			 */
			/*
			 * //if global data, need to send to jobtracker, then the jobtracker
			 * will combine them if(job.getGlobalUniqValuePath() != null) {
			 * LOG.info("send global data to jobtracker!"); sendGlobalData(job,
			 * localfilename, reporter); }
			 */

			// End of clean up.
		} catch (IOException ioe) {
			try {
				reducer.close();
			} catch (IOException ignored) {
			}

			try {
				// localOut.close(reporter);
				filteredOut.close(reporter);
			} catch (IOException ignored) {
			}

			throw ioe;
		}

		long taskend = new Date().getTime();
		// report iterative task information
		IterativeTaskCompletionEvent event = new IterativeTaskCompletionEvent(
				getJobID(), iteration, this.getTaskID(), this.getTaskID()
						.getTaskID().getId(), this.isMapTask());
		event.setProcessedRecords(reduceInputKeyCounter.getCounter());
		event.setRunTime(taskend - taskstart);
		LOG.info("iteration " + iteration + " non converged items number is "
				+ nonConvergedItems);
		event.setSubDistance(nonConvergedItems);

		try {
			umbilical.iterativeTaskComplete(event);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		reducer.iteration_complete(iteration);
	}
	
	@SuppressWarnings("unchecked")
	private <INKEY, INVALUE, OUTKEY, OUTVALUE, SOURCEKEY> void runIncrMRBGReducer(
			JobConf job, TaskUmbilicalProtocol umbilical,
			final TaskReporter reporter, int iteration,
			RawKeyValueSourceIterator rIter, RawComparator<INKEY> comparator,
			Class<INKEY> keyClass, Class<INVALUE> valueClass,
			Class<SOURCEKEY> skeyClass, Class<OUTVALUE> outvalueClass,
			long starttime) throws IOException {

		long taskstart = new Date().getTime();

		IterativeReducer<INKEY, INVALUE, OUTKEY, OUTVALUE> reducer = ReflectionUtils
				.newInstance(job.getIterativeReducerClass(), job);

		/*
		 * boolean writeHDFS = true;
		 * if(job.getOutputKeyClass().equals(GlobalUniqKeyWritable.class))
		 * writeHDFS = false;
		 */

		WrappedIndexOutputLocalCollector<INKEY, INVALUE, OUTKEY, OUTVALUE> collector = new WrappedIndexOutputLocalCollector<INKEY, INVALUE, OUTKEY, OUTVALUE>(
				reporter, null); // now, we skip terminator

		int taskid = this.getTaskID().getTaskID().getId();
		String filteredfilename = new String(job.get("mapred.incr.tmp", "/mnt/iteroop/")
				+ job.getIterativeAlgorithmID() + "/filter-" + iteration + "."
				+ this.getTaskID().getTaskID().getId());
		final RecordWriter<OUTKEY, OUTVALUE> filteredOut = new OldTrackingRecordWriter<OUTKEY, OUTVALUE>(
				reduceOutputCounter, job, reporter, filteredfilename, false);

		Path remotePreservedPath = new Path(job.getPreserveStatePath()
				+ "/preserve-" + taskid);
		Path remotePreservedIndexPath = new Path(job.getPreserveStatePath()
				+ "/preserve-" + taskid + ".index");
		Path preservedPath = new Path(job.get("mapred.incr.tmp", "/mnt/iteroop/")
				+ job.getIterativeAlgorithmID() + "/preserve-" + taskid);
		Path oldPreservedIndexPath = new Path(job.get("mapred.incr.tmp", "/mnt/iteroop/")
				+ job.getIterativeAlgorithmID() + "/preserve-Incr-"
				+ (iteration - 1) + "-" + taskid + ".index");
		Path newPreserveIndexPath = new Path(job.get("mapred.incr.tmp", "/mnt/iteroop/")
				+ job.getIterativeAlgorithmID() + "/preserve-Incr-" + iteration
				+ "-" + taskid + ".index");

		IFile.PreserveFile<INKEY, INVALUE, SOURCEKEY, OUTVALUE> preserveFile;
		if (hdfs.exists(remotePreservedPath)) {
			// if it doesn't exist the local static file, it means it is the
			// first iteration, so copy it from hdfs
			if (!localfs.exists(preservedPath)) {
				hdfs.copyToLocalFile(remotePreservedPath, preservedPath);
				hdfs.copyToLocalFile(remotePreservedIndexPath,
						oldPreservedIndexPath);
				LOG.info("copy remote preserve file " + remotePreservedPath
						+ " to local disk" + preservedPath
						+ "!!!!!!!!! and file size is "
						+ localfs.getFileStatus(preservedPath).getLen());
			}

			// preserveFile = new IFile.PreserveFile<INKEY, INVALUE, SOURCEKEY,
			// OUTVALUE>(conf, preservedPath,
			// oldPreservedIndexPath, newPreserveIndexPath,iteration, keyClass,
			// valueClass, skeyClass, outvalueClass);
			preserveFile = new IFile.PreserveFile<INKEY, INVALUE, SOURCEKEY, OUTVALUE>(
					conf, preservedPath, oldPreservedIndexPath,
					newPreserveIndexPath, keyClass, valueClass, skeyClass,
					outvalueClass, job.getPreserveBufferType() +  10*job.getNumReduceTasks(),job.getPreserveBufferInterval()  );
		} else { 
			throw new IOException(
					"acturally, there is no preserve data on the path you"
							+ " have set! Please check the path and check the map task number "
							+ remotePreservedPath);
		}

		int nonConvergedItems = 0;
		// apply reduce function
		try {
			// increment processed counter only if skipping feature is enabled
			boolean incrProcCount = SkipBadRecords.getReducerMaxSkipGroups(job) > 0
					&& SkipBadRecords.getAutoIncrReducerProcCount(job);

			if (isSkipping()) {
				throw new IOException("should consider skipping case!!!!");
			}

			IncrementalSourceValuesIterator<INKEY, INVALUE, SOURCEKEY, OUTVALUE> values;

			if (job.isBufferReduceKVs()) {
				// this is the in-memory version
				values = new ValuesInMemIterator<INKEY, INVALUE, SOURCEKEY, OUTVALUE>(
						rIter, 0, job.getOutputValueGroupingComparator(),
						keyClass, valueClass, skeyClass, outvalueClass,
						preserveFile, getTaskID().getTaskID().getId(),
						reducer.removeLable(), job, reporter);
			} else {
				// this is the in-file version
				values = new ValuesInFileIterator<INKEY, INVALUE, SOURCEKEY, OUTVALUE>(
						rIter, 0, job.getOutputValueGroupingComparator(),
						keyClass, valueClass, skeyClass, outvalueClass,
						preserveFile, getTaskID().getTaskID().getId(),
						reducer.removeLable(), job, reporter);
			}

			reducePhase.set(rIter.getProgress().get()); // update progress
			reporter.progress();

			long time1 = System.currentTimeMillis();
			int processed = 0;
			while (values.more()) {
				reduceInputKeyCounter.increment(1);

				collector.clearCollectedValue();
				INKEY key = values.getKey();
				reducer.reduce(key, values, collector, reporter);
				//OUTVALUE oldvalue = values.getPreservedOutValue();
				OUTKEY newkey = collector.collectedKey();
				OUTVALUE newvalue = collector.collectedValue();

				// update the result key value pair
				values.updateResKV(key, newvalue);

				/*
				// filter the neglective record, write the filtered records to
				// filteredOut
				float diff = reducer.distance(newkey, oldvalue, newvalue);

				// LOG.info("for key " + key + " source file " + largestPri +
				// " diff between " + latestValue + " and " + value + " is " +
				// diff);

				if (diff >= filter_threshold) {
					// localwriter.write(key, value);
					filteredOut.write(newkey, newvalue);
					// LOG.info("collect " + key + "\t" + newvalue + " diff " +
					// diff);
					nonConvergedItems++;
				} else {
					// LOG.info("skip " + key + "\t" + newvalue + " diff " +
					// diff);
				}
*/
				filteredOut.write(newkey, newvalue);
				nonConvergedItems++;
				
				if (incrProcCount) {
					reporter.incrCounter(SkipBadRecords.COUNTER_GROUP,
							SkipBadRecords.COUNTER_REDUCE_PROCESSED_GROUPS, 1);
				}
				processed++;

				values.nextKey();

				reducePhase.set(rIter.getProgress().get()); // update progress
				reporter.progress();
			}
			long time2 = System.currentTimeMillis();

			LOG.info("iteration " + iteration + " iterating " + processed
					+ " key-value pairs takes " + (time2 - time1) + " total "
					+ (time2 - starttime));
			// for single preserve file
			// values.writeRest();
			// collector.writeRestKVs();

			/**
			 * merging the results and obtain result file with unique keys
			 */
			 boolean writeHDFS = (job.getMaxIterations() == iteration);
//			boolean writeHDFS = false;

			if (writeHDFS) { 
				String resultFile = new String(job.get("mapred.incr.tmp", "/mnt/iteroop/")
						+ job.getIterativeAlgorithmID() + "/result-"
						+ iteration + "."
						+ this.getTaskID().getTaskID().getId());
				final RecordWriter<OUTKEY, OUTVALUE> resultOut = new OldTrackingRecordWriter<OUTKEY, OUTVALUE>(
						reduceOutputCounter, job, reporter, resultFile, false);
				preserveFile.writeResult(resultOut, job);
				filteredOut.close(reporter);
				values.close(); // close the k,sk,v writer
				reducer.close();

				resultOut.close(reporter);
				// dataFile.close();
				this.localfs.delete(new Path(job.get("mapred.incr.tmp", "/mnt/iteroop/")
						+ job.getIterativeAlgorithmID() + "/.result-"
						+ iteration + "."
						+ this.getTaskID().getTaskID().getId() + ".crc"));
				hdfs.copyFromLocalFile(false, new Path(resultFile), new Path(
						job.get("mapred.output.dir") + "/result/iteration-"
								+ iteration + "/" + getOutputName(getPartition())));
			} else {
				filteredOut.close(reporter);
				values.close(); // close the k,sk,v writer
				reducer.close();
			}
			/*
			 * boolean writeHDFS = (job.getMaxIterations() == iteration);
			 * if(writeHDFS){ hdfs.copyFromLocalFile(false, preservedPath,
			 * remotePreservedPath); hdfs.copyFromLocalFile(false,
			 * newPreserveIndexPath, remotePreservedIndexPath); }
			 */
			/*
			 * //if global data, need to send to jobtracker, then the jobtracker
			 * will combine them if(job.getGlobalUniqValuePath() != null) {
			 * LOG.info("send global data to jobtracker!"); sendGlobalData(job,
			 * localfilename, reporter); }
			 */

			// End of clean up.
		} catch (IOException ioe) {
			try {
				reducer.close();
			} catch (IOException ignored) {
			}

			try {
				// localOut.close(reporter);
				filteredOut.close(reporter);
			} catch (IOException ignored) {
			}

			throw ioe;
		}

		long taskend = new Date().getTime();
		// report iterative task information
		IterativeTaskCompletionEvent event = new IterativeTaskCompletionEvent(
				getJobID(), iteration, this.getTaskID(), this.getTaskID()
						.getTaskID().getId(), this.isMapTask());
		event.setProcessedRecords(reduceInputKeyCounter.getCounter());
		event.setRunTime(taskend - taskstart);
		LOG.info("iteration " + iteration + " non converged items number is "
				+ nonConvergedItems);
		event.setSubDistance(nonConvergedItems);

		try {
			umbilical.iterativeTaskComplete(event);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		reducer.iteration_complete(iteration);
	}

	private class NewTrackingRecordWriter<K, V> extends
			org.apache.hadoop.mapreduce.RecordWriter<K, V> {
		private final org.apache.hadoop.mapreduce.RecordWriter<K, V> real;
		private final org.apache.hadoop.mapreduce.Counter outputRecordCounter;
		private final org.apache.hadoop.mapreduce.Counter fileOutputByteCounter;
		private final Statistics fsStats;

		NewTrackingRecordWriter(
				org.apache.hadoop.mapreduce.Counter recordCounter, JobConf job,
				TaskReporter reporter,
				org.apache.hadoop.mapreduce.TaskAttemptContext taskContext)
				throws InterruptedException, IOException {
			this.outputRecordCounter = recordCounter;
			this.fileOutputByteCounter = reporter
					.getCounter(org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.Counter.BYTES_WRITTEN);
			Statistics matchedStats = null;
			// TaskAttemptContext taskContext = new TaskAttemptContext(job,
			// getTaskID());
			if (outputFormat instanceof org.apache.hadoop.mapreduce.lib.output.FileOutputFormat) {
				matchedStats = getFsStatistics(
						org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
								.getOutputPath(taskContext),
						taskContext.getConfiguration());
			}
			fsStats = matchedStats;

			long bytesOutPrev = getOutputBytes(fsStats);
			this.real = (org.apache.hadoop.mapreduce.RecordWriter<K, V>) outputFormat
					.getRecordWriter(taskContext);
			long bytesOutCurr = getOutputBytes(fsStats);
			fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			long bytesOutPrev = getOutputBytes(fsStats);
			real.close(context);
			long bytesOutCurr = getOutputBytes(fsStats);
			fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
		}

		@Override
		public void write(K key, V value) throws IOException,
				InterruptedException {
			long bytesOutPrev = getOutputBytes(fsStats);
			real.write(key, value);
			long bytesOutCurr = getOutputBytes(fsStats);
			fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
			outputRecordCounter.increment(1);
		}

		private long getOutputBytes(Statistics stats) {
			return stats == null ? 0 : stats.getBytesWritten();
		}
	}

	@SuppressWarnings("unchecked")
	private <INKEY, INVALUE, OUTKEY, OUTVALUE> void runNewReducer(JobConf job,
			final TaskUmbilicalProtocol umbilical, final TaskReporter reporter,
			RawKeyValueIterator rIter, RawComparator<INKEY> comparator,
			Class<INKEY> keyClass, Class<INVALUE> valueClass)
			throws IOException, InterruptedException, ClassNotFoundException {
		// wrap value iterator to report progress.
		final RawKeyValueIterator rawIter = rIter;
		rIter = new RawKeyValueIterator() {
			public void close() throws IOException {
				rawIter.close();
			}

			public DataInputBuffer getKey() throws IOException {
				return rawIter.getKey();
			}

			public Progress getProgress() {
				return rawIter.getProgress();
			}

			public DataInputBuffer getValue() throws IOException {
				return rawIter.getValue();
			}

			public boolean next() throws IOException {
				boolean ret = rawIter.next();
				reducePhase.set(rawIter.getProgress().get());
				reporter.progress();
				return ret;
			}
		};
		// make a task context so we can get the classes
		org.apache.hadoop.mapreduce.TaskAttemptContext taskContext = new org.apache.hadoop.mapreduce.TaskAttemptContext(
				job, getTaskID());
		// make a reducer
		org.apache.hadoop.mapreduce.Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE> reducer = (org.apache.hadoop.mapreduce.Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE>) ReflectionUtils
				.newInstance(taskContext.getReducerClass(), job);
		org.apache.hadoop.mapreduce.RecordWriter<OUTKEY, OUTVALUE> trackedRW = new NewTrackingRecordWriter<OUTKEY, OUTVALUE>(
				reduceOutputCounter, job, reporter, taskContext);
		job.setBoolean("mapred.skip.on", isSkipping());
		org.apache.hadoop.mapreduce.Reducer.Context reducerContext = createReduceContext(
				reducer, job, getTaskID(), rIter, reduceInputKeyCounter,
				reduceInputValueCounter, trackedRW, committer, reporter,
				comparator, keyClass, valueClass);
		reducer.run(reducerContext);
		trackedRW.close(reducerContext);
	}

	private static enum CopyOutputErrorType {
		NO_ERROR, READ_ERROR, OTHER_ERROR
	};

	class ReduceCopier<K, V, SK> implements MRConstants {

		/** Reference to the umbilical object */
		private TaskUmbilicalProtocol umbilical;
		private final TaskReporter reporter;

		/** Reference to the task object */

		/** Number of ms before timing out a copy */
		private static final int STALLED_COPY_TIMEOUT = 3 * 60 * 1000;

		/** Max events to fetch in one go from the tasktracker */
		private static final int MAX_EVENTS_TO_FETCH = 10000;

		/**
		 * our reduce task instance
		 */
		private ReduceTask reduceTask;

		/**
		 * the list of map outputs currently being copied
		 */
		private List<MapOutputLocation> scheduledCopies;

		private int taskid;

		/**
		 * the results of dispatched copy attempts
		 */
		private List<CopyResult> copyResults;

		/**
		 * the number of outputs to copy in parallel
		 */
		private int numCopiers;

		/**
		 * a number that is set to the max #fetches we'd schedule and then pause
		 * the schduling
		 */
		private int maxInFlight;

		/**
		 * busy hosts from which copies are being backed off Map of host -> next
		 * contact time
		 */
		private Map<String, Long> penaltyBox;

		/**
		 * the set of unique hosts from which we are copying
		 */
		private Set<String> uniqueHosts;

		/**
		 * A reference to the RamManager for writing the map outputs to.
		 */

		private ShuffleRamManager ramManager;

		/**
		 * A reference to the local file system for writing the map outputs to.
		 */
		private FileSystem localFileSys;

		private FileSystem rfs;
		/**
		 * Number of files to merge at a time
		 */
		private int ioSortFactor;

		/**
		 * A reference to the throwable object (if merge throws an exception)
		 */
		private volatile Throwable mergeThrowable;

		/**
		 * A flag to indicate when to exit localFS merge
		 */
		private volatile boolean exitLocalFSMerge = false;

		/**
		 * A flag to indicate when to exit getMapEvents thread
		 */
		private volatile boolean exitGetMapEvents = false;

		/**
		 * When we accumulate maxInMemOutputs number of files in ram, we
		 * merge/spill
		 */
		private final int maxInMemOutputs;

		/**
		 * Usage threshold for in-memory output accumulation.
		 */
		private final float maxInMemCopyPer;

		/**
		 * Maximum memory usage of map outputs to merge from memory into the
		 * reduce, in bytes.
		 */
		private final long maxInMemReduce;

		/**
		 * The threads for fetching the files.
		 */
		private List<MapOutputCopier> copiers = null;

		/**
		 * The object for metrics reporting.
		 */
		private ShuffleClientInstrumentation shuffleClientMetrics;

		/**
		 * the minimum interval between tasktracker polls
		 */
		private static final long MIN_POLL_INTERVAL = 1000;

		/**
		 * a list of map output locations for fetch retrials
		 */
		private List<MapOutputLocation> retryFetches = new ArrayList<MapOutputLocation>();

		/**
		 * The set of required map outputs
		 */
		private Set<TaskID> copiedMapOutputs = Collections
				.synchronizedSet(new TreeSet<TaskID>());

		/**
		 * The set of obsolete map taskids.
		 */
		private Set<TaskAttemptID> obsoleteMapIds = Collections
				.synchronizedSet(new TreeSet<TaskAttemptID>());

		private Random random = null;

		/**
		 * the max of all the map completion times
		 */
		private int maxMapRuntime;

		/**
		 * Maximum number of fetch-retries per-map before reporting it.
		 */
		private int maxFetchFailuresBeforeReporting;

		/**
		 * Maximum number of fetch failures before reducer aborts.
		 */
		private final int abortFailureLimit;

		/**
		 * Initial penalty time in ms for a fetch failure.
		 */
		private static final long INITIAL_PENALTY = 10000;

		/**
		 * Penalty growth rate for each fetch failure.
		 */
		private static final float PENALTY_GROWTH_RATE = 1.3f;

		/**
		 * Default limit for maximum number of fetch failures before reporting.
		 * reduce task starts fetch map output early, if map output are large,
		 * reduce will fetch a lot of times, and if it orginal setting 10, it
		 * usually beyond the limit and job fail
		 */
		private final static int REPORT_FAILURE_LIMIT = 1024;

		/**
		 * Combiner runner, if a combiner is needed
		 */
		private CombinerRunner combinerRunner;

		/**
		 * Resettable collector used for combine.
		 */
		private CombineOutputCollector combineCollector = null;

		/**
		 * Maximum percent of failed fetch attempt before killing the reduce
		 * task.
		 */
		private static final float MAX_ALLOWED_FAILED_FETCH_ATTEMPT_PERCENT = 0.5f;

		/**
		 * Minimum percent of progress required to keep the reduce alive.
		 */
		private static final float MIN_REQUIRED_PROGRESS_PERCENT = 0.5f;

		/**
		 * Maximum percent of shuffle execution time required to keep the
		 * reducer alive.
		 */
		private static final float MAX_ALLOWED_STALL_TIME_PERCENT = 0.5f;

		/**
		 * Minimum number of map fetch retries.
		 */
		private static final int MIN_FETCH_RETRIES_PER_MAP = 2;

		/**
		 * The minimum percentage of maps yet to be copied, which indicates end
		 * of shuffle
		 */
		private static final float MIN_PENDING_MAPS_PERCENT = 0.25f;
		/**
		 * Maximum no. of unique maps from which we failed to fetch map-outputs
		 * even after {@link #maxFetchRetriesPerMap} retries; after this the
		 * reduce task is failed.
		 */
		private int maxFailedUniqueFetches = 5;

		/**
		 * The maps from which we fail to fetch map-outputs even after
		 * {@link #maxFetchRetriesPerMap} retries.
		 */
		Set<TaskID> fetchFailedMaps = new TreeSet<TaskID>();

		/**
		 * A map of taskId -> no. of failed fetches
		 */
		Map<TaskAttemptID, Integer> mapTaskToFailedFetchesMap = new HashMap<TaskAttemptID, Integer>();

		/**
		 * Initial backoff interval (milliseconds)
		 */
		private static final int BACKOFF_INIT = 4000;

		/**
		 * The interval for logging in the shuffle
		 */
		private static final int MIN_LOG_TIME = 60000;

		/**
		 * List of in-memory map-outputs.
		 */
		private final List<MapOutput> mapOutputsFilesInMemory = Collections
				.synchronizedList(new LinkedList<MapOutput>());

		/**
		 * The map for (Hosts, List of MapIds from this Host) maintaining map
		 * output locations
		 */
		private final Map<String, List<MapOutputLocation>> mapLocations = new ConcurrentHashMap<String, List<MapOutputLocation>>();

		class ShuffleClientInstrumentation implements MetricsSource {
			final MetricsRegistry registry = new MetricsRegistry("shuffleInput");
			final MetricMutableCounterLong inputBytes = registry.newCounter(
					"shuffle_input_bytes", "", 0L);
			final MetricMutableCounterInt failedFetches = registry.newCounter(
					"shuffle_failed_fetches", "", 0);
			final MetricMutableCounterInt successFetches = registry.newCounter(
					"shuffle_success_fetches", "", 0);
			private volatile int threadsBusy = 0;

			@SuppressWarnings("deprecation")
			ShuffleClientInstrumentation(JobConf conf) {
				registry.tag("user", "User name", conf.getUser())
						.tag("jobName", "Job name", conf.getJobName())
						.tag("jobId", "Job ID",
								ReduceTask.this.getJobID().toString())
						.tag("taskId", "Task ID", getTaskID().toString())
						.tag("sessionId", "Session ID", conf.getSessionId());
			}

			// @Override
			void inputBytes(long numBytes) {
				inputBytes.incr(numBytes);
			}

			// @Override
			void failedFetch() {
				failedFetches.incr();
			}

			// @Override
			void successFetch() {
				successFetches.incr();
			}

			// @Override
			synchronized void threadBusy() {
				++threadsBusy;
			}

			// @Override
			synchronized void threadFree() {
				--threadsBusy;
			}

			@Override
			public void getMetrics(MetricsBuilder builder, boolean all) {
				MetricsRecordBuilder rb = builder.addRecord(registry.name());
				rb.addGauge("shuffle_fetchers_busy_percent", "",
						numCopiers == 0 ? 0 : 100. * threadsBusy / numCopiers);
				registry.snapshot(rb, all);
			}

		}

		private ShuffleClientInstrumentation createShuffleClientInstrumentation() {
			return DefaultMetricsSystem.INSTANCE.register(
					"ShuffleClientMetrics", "Shuffle input metrics",
					new ShuffleClientInstrumentation(conf));
		}

		/** Represents the result of an attempt to copy a map output */
		private class CopyResult {

			// the map output location against which a copy attempt was made
			private final MapOutputLocation loc;

			// the size of the file copied, -1 if the transfer failed
			private final long size;

			// a flag signifying whether a copy result is obsolete
			private static final int OBSOLETE = -2;

			private CopyOutputErrorType error = CopyOutputErrorType.NO_ERROR;

			CopyResult(MapOutputLocation loc, long size) {
				this.loc = loc;
				this.size = size;
			}

			CopyResult(MapOutputLocation loc, long size,
					CopyOutputErrorType error) {
				this.loc = loc;
				this.size = size;
				this.error = error;
			}

			public boolean getSuccess() {
				return size >= 0;
			}

			public boolean isObsolete() {
				return size == OBSOLETE;
			}

			public long getSize() {
				return size;
			}

			public String getHost() {
				return loc.getHost();
			}

			public MapOutputLocation getLocation() {
				return loc;
			}

			public CopyOutputErrorType getError() {
				return error;
			}
		}

		private int nextMapOutputCopierId = 0;
		private boolean reportReadErrorImmediately;

		/**
		 * Abstraction to track a map-output.
		 */
		private class MapOutputLocation {
			TaskAttemptID taskAttemptId;
			TaskID taskId;
			String ttHost;
			URL taskOutput;

			public MapOutputLocation(TaskAttemptID taskAttemptId,
					String ttHost, URL taskOutput) {
				this.taskAttemptId = taskAttemptId;
				this.taskId = this.taskAttemptId.getTaskID();
				this.ttHost = ttHost;
				this.taskOutput = taskOutput;
			}

			public TaskAttemptID getTaskAttemptId() {
				return taskAttemptId;
			}

			public TaskID getTaskId() {
				return taskId;
			}

			public String getHost() {
				return ttHost;
			}

			public URL getOutputLocation() {
				return taskOutput;
			}

			@Override
			public String toString() {
				return taskAttemptId + "\t" + ttHost + "\t" + taskOutput;
			}
		}

		/** Describes the output of a map; could either be on disk or in-memory. */
		private class MapOutput {
			final TaskID mapId;
			final TaskAttemptID mapAttemptId;

			final Path file;
			final Configuration conf;

			byte[] data;
			final boolean inMemory;
			long compressedSize;

			public MapOutput(TaskID mapId, TaskAttemptID mapAttemptId,
					Configuration conf, Path file, long size) {
				this.mapId = mapId;
				this.mapAttemptId = mapAttemptId;

				this.conf = conf;
				this.file = file;
				this.compressedSize = size;

				this.data = null;

				this.inMemory = false;
			}

			public MapOutput(TaskID mapId, TaskAttemptID mapAttemptId,
					byte[] data, int compressedLength) {
				this.mapId = mapId;
				this.mapAttemptId = mapAttemptId;

				this.file = null;
				this.conf = null;

				this.data = data;
				this.compressedSize = compressedLength;

				this.inMemory = true;
			}

			public void discard() throws IOException {
				if (inMemory) {
					data = null;
				} else {
					FileSystem fs = file.getFileSystem(conf);
					fs.delete(file, true);
				}
			}
		}

		class ShuffleRamManager implements RamManager {
			/*
			 * Maximum percentage of the in-memory limit that a single shuffle
			 * can consume
			 */
			private static final float MAX_SINGLE_SHUFFLE_SEGMENT_FRACTION = 0.25f;

			/*
			 * Maximum percentage of shuffle-threads which can be stalled
			 * simultaneously after which a merge is triggered.
			 */
			private static final float MAX_STALLED_SHUFFLE_THREADS_FRACTION = 0.75f;

			private final long maxSize;
			private final long maxSingleShuffleLimit;

			private long size = 0;

			private Object dataAvailable = new Object();
			private long fullSize = 0;
			private int numPendingRequests = 0;
			private int numRequiredMapOutputs = 0;
			private int numClosed = 0;
			private boolean closed = false;

			public ShuffleRamManager(Configuration conf) throws IOException {
				final float maxInMemCopyUse = conf.getFloat(
						"mapred.job.shuffle.input.buffer.percent", 0.70f);
				if (maxInMemCopyUse > 1.0 || maxInMemCopyUse < 0.0) {
					throw new IOException(
							"mapred.job.shuffle.input.buffer.percent"
									+ maxInMemCopyUse);
				}
				// Allow unit tests to fix Runtime memory
				maxSize = (int) (conf.getInt(
						"mapred.job.reduce.total.mem.bytes", (int) Math.min(
								Runtime.getRuntime().maxMemory(),
								Integer.MAX_VALUE)) * maxInMemCopyUse);
				maxSingleShuffleLimit = (long) (maxSize * MAX_SINGLE_SHUFFLE_SEGMENT_FRACTION);
				LOG.info("ShuffleRamManager: MemoryLimit=" + maxSize
						+ ", MaxSingleShuffleLimit=" + maxSingleShuffleLimit);
			}

			public synchronized boolean reserve(int requestedSize,
					InputStream in) throws InterruptedException {
				// Wait till the request can be fulfilled...
				while ((size + requestedSize) > maxSize) {

					// Close the input...
					if (in != null) {
						try {
							in.close();
						} catch (IOException ie) {
							LOG.info("Failed to close connection with: " + ie);
						} finally {
							in = null;
						}
					}

					// Track pending requests
					synchronized (dataAvailable) {
						++numPendingRequests;
						dataAvailable.notify();
					}

					// Wait for memory to free up
					wait();

					// Track pending requests
					synchronized (dataAvailable) {
						--numPendingRequests;
					}
				}

				size += requestedSize;

				return (in != null);
			}

			public synchronized void unreserve(int requestedSize) {
				size -= requestedSize;

				synchronized (dataAvailable) {
					fullSize -= requestedSize;
					--numClosed;
				}

				// Notify the threads blocked on RamManager.reserve
				notifyAll();
			}

			public boolean waitForDataToMerge() throws InterruptedException {
				boolean done = false;
				synchronized (dataAvailable) {
					// Start in-memory merge if manager has been closed or...
					while (!closed
							&&
							// In-memory threshold exceeded and at least two
							// segments
							// have been fetched
							(getPercentUsed() < maxInMemCopyPer || numClosed < 2)
							&&
							// More than "mapred.inmem.merge.threshold" map
							// outputs
							// have been fetched into memory
							(maxInMemOutputs <= 0 || numClosed < maxInMemOutputs)
							&&
							// More than MAX... threads are blocked on the
							// RamManager
							// or the blocked threads are the last map outputs
							// to be
							// fetched. If numRequiredMapOutputs is zero, either
							// setNumCopiedMapOutputs has not been called (no
							// map ouputs
							// have been fetched, so there is nothing to merge)
							// or the
							// last map outputs being transferred without
							// contention, so a merge would be premature.
							(numPendingRequests < numCopiers
									* MAX_STALLED_SHUFFLE_THREADS_FRACTION && (0 == numRequiredMapOutputs || numPendingRequests < numRequiredMapOutputs))) {
						dataAvailable.wait();
					}
					done = closed;
				}
				return done;
			}

			public void closeInMemoryFile(int requestedSize) {
				synchronized (dataAvailable) {
					fullSize += requestedSize;
					++numClosed;
					dataAvailable.notify();
				}
			}

			public void setNumCopiedMapOutputs(int numRequiredMapOutputs) {
				synchronized (dataAvailable) {
					this.numRequiredMapOutputs = numRequiredMapOutputs;
					dataAvailable.notify();
				}
			}

			public void close() {
				synchronized (dataAvailable) {
					closed = true;
					LOG.info("Closed ram manager");
					dataAvailable.notify();
				}
			}

			private float getPercentUsed() {
				return (float) fullSize / maxSize;
			}

			boolean canFitInMemory(long requestedSize) {
				return (requestedSize < Integer.MAX_VALUE && requestedSize < maxSingleShuffleLimit);
			}
		}

		/** Copies map outputs as they become available */
		private class MapOutputCopier extends Thread {
			// basic/unit connection timeout (in milliseconds)
			private final static int UNIT_CONNECT_TIMEOUT = 30 * 1000;
			// default read timeout (in milliseconds)
			private final static int DEFAULT_READ_TIMEOUT = 3 * 60 * 1000;
			private final int shuffleConnectionTimeout;
			private final int shuffleReadTimeout;

			private MapOutputLocation currentLocation = null;
			private int id = nextMapOutputCopierId++;
			private Reporter reporter;
			private boolean readError = false;

			// Decompression of map-outputs
			private CompressionCodec codec = null;
			private Decompressor decompressor = null;

			private final SecretKey jobTokenSecret;

			public MapOutputCopier(JobConf job, Reporter reporter,
					SecretKey jobTokenSecret) {
				setName("MapOutputCopier " + reduceTask.getTaskID() + "." + id);
				LOG.debug(getName() + " created");
				this.reporter = reporter;

				this.jobTokenSecret = jobTokenSecret;

				shuffleConnectionTimeout = job.getInt(
						"mapreduce.reduce.shuffle.connect.timeout",
						STALLED_COPY_TIMEOUT);
				shuffleReadTimeout = job.getInt(
						"mapreduce.reduce.shuffle.read.timeout",
						DEFAULT_READ_TIMEOUT);

				if (job.getCompressMapOutput()) {
					Class<? extends CompressionCodec> codecClass = job
							.getMapOutputCompressorClass(DefaultCodec.class);
					codec = ReflectionUtils.newInstance(codecClass, job);
					decompressor = CodecPool.getDecompressor(codec);
				}
			}

			/**
			 * Fail the current file that we are fetching
			 * 
			 * @return were we currently fetching?
			 */
			public synchronized boolean fail() {
				if (currentLocation != null) {
					finish(-1, CopyOutputErrorType.OTHER_ERROR);
					return true;
				} else {
					return false;
				}
			}

			/**
			 * Get the current map output location.
			 */
			public synchronized MapOutputLocation getLocation() {
				return currentLocation;
			}

			private synchronized void start(MapOutputLocation loc) {
				currentLocation = loc;
			}

			private synchronized void finish(long size,
					CopyOutputErrorType error) {
				if (currentLocation != null) {
					LOG.debug(getName() + " finishing " + currentLocation
							+ " =" + size);
					synchronized (copyResults) {
						copyResults.add(new CopyResult(currentLocation, size,
								error));
						copyResults.notify();
					}
					currentLocation = null;
				}
			}

			/**
			 * Loop forever and fetch map outputs as they become available. The
			 * thread exits when it is interrupted by {@link ReduceTaskRunner}
			 */
			@Override
			public void run() {
				int i = 1;
				while (true) {
					try {
						MapOutputLocation loc = null;
						long size = -1;

						synchronized (scheduledCopies) {
							while (scheduledCopies.isEmpty()) {
								scheduledCopies.wait();
							}
							loc = scheduledCopies.remove(0);
							LOG.debug("i am awake for processing " + loc);
						}
						CopyOutputErrorType error = CopyOutputErrorType.OTHER_ERROR;
						readError = false;
						try {
							LOG.info("creating shuffle client " + loc);
							shuffleClientMetrics = createShuffleClientInstrumentation();
							shuffleClientMetrics.threadBusy();
							start(loc);
							size = copyOutput(loc);
							shuffleClientMetrics.successFetch();
							error = CopyOutputErrorType.NO_ERROR;
						} catch (IOException e) {
							LOG.warn(reduceTask.getTaskID() + " copy failed: "
									+ loc.getTaskAttemptId() + " from "
									+ loc.getHost());
							LOG.warn("lalalala "
									+ StringUtils.stringifyException(e));
							shuffleClientMetrics.failedFetch();
							if (readError) {
								error = CopyOutputErrorType.READ_ERROR;
							}
							// Reset
							size = -1;
						} finally {
							shuffleClientMetrics.threadFree();
							finish(size, error);

							/**
							 * //scheduledCopies.clear(); this is a bug for
							 * hadoop, for small map tasks, this offen results
							 * in map output copy failed zhangyf has fixed it
							 * with the following change
							 */
							scheduledCopies.remove(loc);
						}
					} catch (InterruptedException e) {
						break; // ALL DONE
					} catch (FSError e) {
						LOG.error("Task: " + reduceTask.getTaskID()
								+ " - FSError: "
								+ StringUtils.stringifyException(e));
						try {
							umbilical.fsError(reduceTask.getTaskID(),
									e.getMessage(), jvmContext);
						} catch (IOException io) {
							LOG.error("Could not notify TT of FSError: "
									+ StringUtils.stringifyException(io));
						}
					} catch (Throwable th) {
						String msg = getTaskID()
								+ " : Map output copy failure : "
								+ StringUtils.stringifyException(th);
						reportFatalError(getTaskID(), th, msg);
					}
				}

				if (decompressor != null) {
					CodecPool.returnDecompressor(decompressor);
				}

			}

			/**
			 * Copies a a map output from a remote host, via HTTP.
			 * 
			 * @param currentLocation
			 *            the map output location to be copied
			 * @return the path (fully qualified) of the copied file
			 * @throws IOException
			 *             if there is an error copying the file
			 * @throws InterruptedException
			 *             if the copier should give up
			 */
			private long copyOutput(MapOutputLocation loc) throws IOException,
					InterruptedException {
				// check if we still need to copy the output from this location
				if (copiedMapOutputs.contains(loc.getTaskId())
						|| obsoleteMapIds.contains(loc.getTaskAttemptId())) {
					return CopyResult.OBSOLETE;
				}

				// a temp filename. If this file gets created in ramfs, we're
				// fine,
				// else, we will check the localFS to find a suitable final
				// location
				// for this path
				TaskAttemptID reduceId = reduceTask.getTaskID();
				Path filename = new Path(String.format(
						MapOutputFile.REDUCE_INPUT_FILE_FORMAT_STRING,
						TaskTracker.OUTPUT, loc.getTaskId().getId()));

				// Copy the map output to a temp file whose name is unique to
				// this attempt
				Path tmpMapOutput = new Path(filename + "-" + id);

				// Copy the map output
				MapOutput mapOutput = getMapOutput(loc, tmpMapOutput, reduceId
						.getTaskID().getId());
				if (mapOutput == null) {
					throw new IOException("Failed to fetch map-output for "
							+ loc.getTaskAttemptId() + " from " + loc.getHost());
				}

				// The size of the map-output
				long bytes = mapOutput.compressedSize;

				// lock the ReduceTask while we do the rename
				synchronized (ReduceTask.this) {
					if (copiedMapOutputs.contains(loc.getTaskId())) {
						mapOutput.discard();
						return CopyResult.OBSOLETE;
					}

					// Special case: discard empty map-outputs
					if (bytes == 0) {
						try {
							mapOutput.discard();
						} catch (IOException ioe) {
							LOG.info("Couldn't discard output of "
									+ loc.getTaskId());
						}

						// Note that we successfully copied the map-output
						noteCopiedMapOutput(loc.getTaskId());

						return bytes;
					}

					// Process map-output
					if (mapOutput.inMemory) {
						// Save it in the synchronized list of map-outputs
						mapOutputsFilesInMemory.add(mapOutput);
					} else {
						// Rename the temporary file to the final file;
						// ensure it is on the same partition
						tmpMapOutput = mapOutput.file;
						filename = new Path(tmpMapOutput.getParent(),
								filename.getName());
						if (!localFileSys.rename(tmpMapOutput, filename)) {
							localFileSys.delete(tmpMapOutput, true);
							bytes = -1;
							throw new IOException(
									"Failed to rename map output "
											+ tmpMapOutput + " to " + filename);
						}

						synchronized (mapOutputFilesOnDisk) {
							addToMapOutputFilesOnDisk(localFileSys
									.getFileStatus(filename));
						}
					}

					// Note that we successfully copied the map-output
					noteCopiedMapOutput(loc.getTaskId());
				}

				return bytes;
			}

			/**
			 * Save the map taskid whose output we just copied. This function
			 * assumes that it has been synchronized on ReduceTask.this.
			 * 
			 * @param taskId
			 *            map taskid
			 */
			private void noteCopiedMapOutput(TaskID taskId) {
				copiedMapOutputs.add(taskId);
				ramManager.setNumCopiedMapOutputs(numMaps
						- copiedMapOutputs.size());
			}

			/**
			 * Get the map output into a local file (either in the inmemory fs
			 * or on the local fs) from the remote server. We use the file
			 * system so that we generate checksum files on the data.
			 * 
			 * @param mapOutputLoc
			 *            map-output to be fetched
			 * @param filename
			 *            the filename to write the data into
			 * @param connectionTimeout
			 *            number of milliseconds for connection timeout
			 * @param readTimeout
			 *            number of milliseconds for read timeout
			 * @return the path of the file that got created
			 * @throws IOException
			 *             when something goes wrong
			 */
			private MapOutput getMapOutput(MapOutputLocation mapOutputLoc,
					Path filename, int reduce) throws IOException,
					InterruptedException {
				// Connect
				URL url = mapOutputLoc.getOutputLocation();
				URLConnection connection = url.openConnection();

				LOG.info("set secure connection to "
						+ mapOutputLoc.getTaskAttemptId() + " file " + filename);
				InputStream input = setupSecureConnection(mapOutputLoc,
						connection);

				// Validate header from map output
				TaskAttemptID mapId = null;
				try {
					mapId = TaskAttemptID.forName(connection
							.getHeaderField(FROM_MAP_TASK));
				} catch (IllegalArgumentException ia) {
					LOG.warn("Invalid map id ", ia);
					return null;
				}
				TaskAttemptID expectedMapId = mapOutputLoc.getTaskAttemptId();
				if (!mapId.equals(expectedMapId)) {
					LOG.warn("data from wrong map:" + mapId
							+ " arrived to reduce task " + reduce
							+ ", where as expected map output should be from "
							+ expectedMapId);
					return null;
				}

				long decompressedLength = Long.parseLong(connection
						.getHeaderField(RAW_MAP_OUTPUT_LENGTH));
				long compressedLength = Long.parseLong(connection
						.getHeaderField(MAP_OUTPUT_LENGTH));

				if (compressedLength < 0 || decompressedLength < 0) {
					LOG.warn(getName()
							+ " invalid lengths in map output header: id: "
							+ mapId + " compressed len: " + compressedLength
							+ ", decompressed len: " + decompressedLength);
					return null;
				}
				int forReduce = (int) Integer.parseInt(connection
						.getHeaderField(FOR_REDUCE_TASK));

				if (forReduce != reduce) {
					LOG.warn("data for the wrong reduce: " + forReduce
							+ " with compressed len: " + compressedLength
							+ ", decompressed len: " + decompressedLength
							+ " arrived to reduce task " + reduce);
					return null;
				}
				if (LOG.isDebugEnabled()) {
					LOG.debug("header: " + mapId + ", compressed len: "
							+ compressedLength + ", decompressed len: "
							+ decompressedLength);
				}

				// We will put a file in memory if it meets certain criteria:
				// 1. The size of the (decompressed) file should be less than
				// 25% of
				// the total inmem fs
				// 2. There is space available in the inmem fs

				// Check if this map-output can be saved in-memory
				boolean shuffleInMemory = ramManager
						.canFitInMemory(decompressedLength);

				// Shuffle
				MapOutput mapOutput = null;
				if (shuffleInMemory) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Shuffling " + decompressedLength
								+ " bytes (" + compressedLength
								+ " raw bytes) " + "into RAM from "
								+ mapOutputLoc.getTaskAttemptId());
					}

					mapOutput = shuffleInMemory(mapOutputLoc, connection,
							input, (int) decompressedLength,
							(int) compressedLength);
				} else {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Shuffling " + decompressedLength
								+ " bytes (" + compressedLength
								+ " raw bytes) " + "into Local-FS from "
								+ mapOutputLoc.getTaskAttemptId());
					}

					mapOutput = shuffleToDisk(mapOutputLoc, input, filename,
							compressedLength);
				}

				return mapOutput;
			}

			private InputStream setupSecureConnection(
					MapOutputLocation mapOutputLoc, URLConnection connection)
					throws IOException {

				// generate hash of the url
				String msgToEncode = SecureShuffleUtils.buildMsgFrom(connection
						.getURL());
				String encHash = SecureShuffleUtils.hashFromString(msgToEncode,
						jobTokenSecret);

				// put url hash into http header
				connection.setRequestProperty(
						SecureShuffleUtils.HTTP_HEADER_URL_HASH, encHash);

				InputStream input = getInputStream(connection,
						shuffleConnectionTimeout, shuffleReadTimeout);

				// get the replyHash which is HMac of the encHash we sent to the
				// server
				String replyHash = connection
						.getHeaderField(SecureShuffleUtils.HTTP_HEADER_REPLY_URL_HASH);
				if (replyHash == null) {
					throw new IOException(
							"security validation of TT Map output failed");
				}
				if (LOG.isDebugEnabled())
					LOG.debug("url=" + msgToEncode + ";encHash=" + encHash
							+ ";replyHash=" + replyHash);
				// verify that replyHash is HMac of encHash
				SecureShuffleUtils.verifyReply(replyHash, encHash,
						jobTokenSecret);
				if (LOG.isDebugEnabled())
					LOG.debug("for url=" + msgToEncode
							+ " sent hash and receievd reply");
				return input;
			}

			/**
			 * The connection establishment is attempted multiple times and is
			 * given up only on the last failure. Instead of connecting with a
			 * timeout of X, we try connecting with a timeout of x < X but
			 * multiple times.
			 */
			private InputStream getInputStream(URLConnection connection,
					int connectionTimeout, int readTimeout) throws IOException {
				int unit = 0;
				if (connectionTimeout < 0) {
					throw new IOException("Invalid timeout " + "[timeout = "
							+ connectionTimeout + " ms]");
				} else if (connectionTimeout > 0) {
					unit = (UNIT_CONNECT_TIMEOUT > connectionTimeout) ? connectionTimeout
							: UNIT_CONNECT_TIMEOUT;
				}
				// set the read timeout to the total timeout
				connection.setReadTimeout(readTimeout);
				// set the connect timeout to the unit-connect-timeout
				connection.setConnectTimeout(unit);
				while (true) {
					try {
						LOG.info("what is connection? " + connection);
						connection.connect();
						break;
					} catch (IOException ioe) {
						// update the total remaining connect-timeout
						connectionTimeout -= unit;

						// throw an exception if we have waited for timeout
						// amount of time
						// note that the updated value if timeout is used here
						if (connectionTimeout == 0) {
							throw ioe;
						}

						// reset the connect timeout for the last try
						if (connectionTimeout < unit) {
							unit = connectionTimeout;
							// reset the connect time out for the final connect
							connection.setConnectTimeout(unit);
						}
					}
				}
				try {
					return connection.getInputStream();
				} catch (IOException ioe) {
					readError = true;
					throw ioe;
				}
			}

			private MapOutput shuffleInMemory(MapOutputLocation mapOutputLoc,
					URLConnection connection, InputStream input,
					int mapOutputLength, int compressedLength)
					throws IOException, InterruptedException {
				// Reserve ram for the map-output
				boolean createdNow = ramManager.reserve(mapOutputLength, input);

				// Reconnect if we need to
				if (!createdNow) {
					// Reconnect
					try {
						LOG.info("connect " + connection + " map location: "
								+ mapOutputLoc + " mapoutputlength "
								+ mapOutputLength + " compressedLength "
								+ compressedLength);
						connection = mapOutputLoc.getOutputLocation()
								.openConnection();
						input = setupSecureConnection(mapOutputLoc, connection);
					} catch (IOException ioe) {
						LOG.info("Failed reopen connection to fetch map-output from "
								+ mapOutputLoc.getHost());

						// Inform the ram-manager
						ramManager.closeInMemoryFile(mapOutputLength);
						ramManager.unreserve(mapOutputLength);

						throw ioe;
					}
				}

				IFileInputStream checksumIn = new IFileInputStream(input,
						compressedLength);

				input = checksumIn;

				// Are map-outputs compressed?
				if (codec != null) {
					decompressor.reset();
					input = codec.createInputStream(input, decompressor);
				}

				// Copy map-output into an in-memory buffer
				byte[] shuffleData = new byte[mapOutputLength];
				MapOutput mapOutput = new MapOutput(mapOutputLoc.getTaskId(),
						mapOutputLoc.getTaskAttemptId(), shuffleData,
						compressedLength);

				int bytesRead = 0;
				try {
					// LOG.info("read shuffle data " + shuffleData.length + "\t"
					// + mapOutputLength);
					int n = input.read(shuffleData, 0, shuffleData.length);
					while (n > 0) {
						bytesRead += n;
						shuffleClientMetrics.inputBytes(n);

						// LOG.info("read " + bytesRead);
						// indicate we're making progress
						reporter.progress();
						n = input.read(shuffleData, bytesRead,
								(shuffleData.length - bytesRead));
					}

					if (LOG.isDebugEnabled()) {
						LOG.debug("Read " + bytesRead
								+ " bytes from map-output for "
								+ mapOutputLoc.getTaskAttemptId());
					}

					input.close();
				} catch (IOException ioe) {
					LOG.info(
							"Failed to shuffle from "
									+ mapOutputLoc.getTaskAttemptId(), ioe);

					// Inform the ram-manager
					ramManager.closeInMemoryFile(mapOutputLength);
					ramManager.unreserve(mapOutputLength);

					// Discard the map-output
					try {
						mapOutput.discard();
					} catch (IOException ignored) {
						LOG.info("Failed to discard map-output from "
								+ mapOutputLoc.getTaskAttemptId(), ignored);
					}
					mapOutput = null;

					// Close the streams
					IOUtils.cleanup(LOG, input);

					// Re-throw
					readError = true;
					throw ioe;
				}

				// Close the in-memory file
				ramManager.closeInMemoryFile(mapOutputLength);

				// Sanity check
				if (bytesRead != mapOutputLength) {
					// Inform the ram-manager
					ramManager.unreserve(mapOutputLength);

					// Discard the map-output
					try {
						mapOutput.discard();
					} catch (IOException ignored) {
						// IGNORED because we are cleaning up
						LOG.info("Failed to discard map-output from "
								+ mapOutputLoc.getTaskAttemptId(), ignored);
					}
					mapOutput = null;

					throw new IOException("Incomplete map output received for "
							+ mapOutputLoc.getTaskAttemptId() + " from "
							+ mapOutputLoc.getOutputLocation() + " ("
							+ bytesRead + " instead of " + mapOutputLength
							+ ")");
				}

				// TODO: Remove this after a 'fix' for HADOOP-3647
				if (LOG.isDebugEnabled()) {
					if (mapOutputLength > 0) {
						DataInputBuffer dib = new DataInputBuffer();
						dib.reset(shuffleData, 0, shuffleData.length);
						LOG.debug("Rec #1 from "
								+ mapOutputLoc.getTaskAttemptId() + " -> ("
								+ WritableUtils.readVInt(dib) + ", "
								+ WritableUtils.readVInt(dib) + ") from "
								+ mapOutputLoc.getHost());
					}
				}

				return mapOutput;
			}

			private MapOutput shuffleToDisk(MapOutputLocation mapOutputLoc,
					InputStream input, Path filename, long mapOutputLength)
					throws IOException {
				// Find out a suitable location for the output on
				// local-filesystem
				Path localFilename = lDirAlloc.getLocalPathForWrite(filename
						.toUri().getPath(), mapOutputLength, conf);

				MapOutput mapOutput = new MapOutput(mapOutputLoc.getTaskId(),
						mapOutputLoc.getTaskAttemptId(), conf,
						localFileSys.makeQualified(localFilename),
						mapOutputLength);

				// Copy data to local-disk
				OutputStream output = null;
				long bytesRead = 0;
				try {
					output = rfs.create(localFilename);

					byte[] buf = new byte[64 * 1024];
					int n = -1;
					try {
						n = input.read(buf, 0, buf.length);
					} catch (IOException ioe) {
						readError = true;
						throw ioe;
					}
					while (n > 0) {
						bytesRead += n;
						shuffleClientMetrics.inputBytes(n);
						output.write(buf, 0, n);

						// indicate we're making progress
						reporter.progress();
						try {
							n = input.read(buf, 0, buf.length);
						} catch (IOException ioe) {
							readError = true;
							throw ioe;
						}
					}

					LOG.info("Read " + bytesRead
							+ " bytes from map-output for "
							+ mapOutputLoc.getTaskAttemptId());

					output.close();
					input.close();
				} catch (IOException ioe) {
					LOG.info(
							"Failed to shuffle from "
									+ mapOutputLoc.getTaskAttemptId(), ioe);

					// Discard the map-output
					try {
						mapOutput.discard();
					} catch (IOException ignored) {
						LOG.info("Failed to discard map-output from "
								+ mapOutputLoc.getTaskAttemptId(), ignored);
					}
					mapOutput = null;

					// Close the streams
					IOUtils.cleanup(LOG, input, output);

					// Re-throw
					throw ioe;
				}

				// Sanity check
				if (bytesRead != mapOutputLength) {
					try {
						mapOutput.discard();
					} catch (Exception ioe) {
						// IGNORED because we are cleaning up
						LOG.info("Failed to discard map-output from "
								+ mapOutputLoc.getTaskAttemptId(), ioe);
					} catch (Throwable t) {
						String msg = getTaskID()
								+ " : Failed in shuffle to disk :"
								+ StringUtils.stringifyException(t);
						reportFatalError(getTaskID(), t, msg);
					}
					mapOutput = null;

					throw new IOException("Incomplete map output received for "
							+ mapOutputLoc.getTaskAttemptId() + " from "
							+ mapOutputLoc.getOutputLocation() + " ("
							+ bytesRead + " instead of " + mapOutputLength
							+ ")");
				}

				return mapOutput;

			}

		} // MapOutputCopier

		private void configureClasspath(JobConf conf) throws IOException {

			// get the task and the current classloader which will become the
			// parent
			Task task = ReduceTask.this;
			ClassLoader parent = conf.getClassLoader();

			// get the work directory which holds the elements we are
			// dynamically
			// adding to the classpath
			File workDir = new File(task.getJobFile()).getParentFile();
			ArrayList<URL> urllist = new ArrayList<URL>();

			// add the jars and directories to the classpath
			String jar = conf.getJar();
			if (jar != null) {
				File jobCacheDir = new File(new Path(jar).getParent()
						.toString());

				File[] libs = new File(jobCacheDir, "lib").listFiles();
				if (libs != null) {
					for (int i = 0; i < libs.length; i++) {
						urllist.add(libs[i].toURL());
					}
				}
				urllist.add(new File(jobCacheDir, "classes").toURL());
				urllist.add(jobCacheDir.toURL());

			}
			urllist.add(workDir.toURL());

			// create a new classloader with the old classloader as its parent
			// then set that classloader as the one used by the current jobconf
			URL[] urls = urllist.toArray(new URL[urllist.size()]);
			URLClassLoader loader = new URLClassLoader(urls, parent);
			conf.setClassLoader(loader);
		}

		public ReduceCopier(TaskUmbilicalProtocol umbilical, JobConf conf,
				TaskReporter reporter) throws ClassNotFoundException,
				IOException {

			configureClasspath(conf);
			this.reporter = reporter;
			this.shuffleClientMetrics = createShuffleClientInstrumentation();
			this.umbilical = umbilical;
			this.reduceTask = ReduceTask.this;
			this.scheduledCopies = new ArrayList<MapOutputLocation>(100);
			this.copyResults = new ArrayList<CopyResult>(100);
			this.numCopiers = conf.getInt("mapred.reduce.parallel.copies", 5);
			this.maxInFlight = 4 * numCopiers;
			Counters.Counter combineInputCounter = reporter
					.getCounter(Task.Counter.COMBINE_INPUT_RECORDS);
			this.combinerRunner = CombinerRunner.create(conf, getTaskID(),
					combineInputCounter, reporter, null);
			if (combinerRunner != null) {
				combineCollector = new CombineOutputCollector(
						reduceCombineOutputCounter, reporter, conf);
			}

			this.ioSortFactor = conf.getInt("io.sort.factor", 10);

			this.abortFailureLimit = Math.max(30, numMaps / 10);

			this.maxFetchFailuresBeforeReporting = conf.getInt(
					"mapreduce.reduce.shuffle.maxfetchfailures",
					REPORT_FAILURE_LIMIT);

			this.maxFailedUniqueFetches = Math.min(numMaps,
					this.maxFailedUniqueFetches);
			this.maxInMemOutputs = conf.getInt("mapred.inmem.merge.threshold",
					1000);
			this.maxInMemCopyPer = conf.getFloat(
					"mapred.job.shuffle.merge.percent", 0.66f);
			final float maxRedPer = conf.getFloat(
					"mapred.job.reduce.input.buffer.percent", 0f);
			if (maxRedPer > 1.0 || maxRedPer < 0.0) {
				throw new IOException("mapred.job.reduce.input.buffer.percent"
						+ maxRedPer);
			}
			this.maxInMemReduce = (int) Math.min(Runtime.getRuntime()
					.maxMemory() * maxRedPer, Integer.MAX_VALUE);

			// Setup the RamManager
			ramManager = new ShuffleRamManager(conf);

			localFileSys = FileSystem.getLocal(conf);

			rfs = ((LocalFileSystem) localFileSys).getRaw();

			// hosts -> next contact time
			this.penaltyBox = new LinkedHashMap<String, Long>();

			// hostnames
			this.uniqueHosts = new HashSet<String>();

			// Seed the random number generator with a reasonably globally
			// unique seed
			long randomSeed = System.nanoTime()
					+ (long) Math.pow(this.reduceTask.getPartition(),
							(this.reduceTask.getPartition() % 10));
			this.random = new Random(randomSeed);
			this.maxMapRuntime = 0;
			this.reportReadErrorImmediately = conf.getBoolean(
					"mapreduce.reduce.shuffle.notify.readerror", true);
		}

		private boolean busyEnough(int numInFlight) {
			return numInFlight > maxInFlight;
		}

		public boolean fetchOutputs() throws IOException {
			int totalFailures = 0;
			int numInFlight = 0, numCopied = 0;
			DecimalFormat mbpsFormat = new DecimalFormat("0.00");
			final Progress copyPhase = reduceTask.getProgress().phase();
			LocalFSMerger localFSMergerThread = null;
			InMemFSMergeThread inMemFSMergeThread = null;
			GetMapEventsThread getMapEventsThread = null;

			for (int i = 0; i < numMaps; i++) {
				copyPhase.addPhase(); // add sub-phase per file
			}

			copiers = new ArrayList<MapOutputCopier>(numCopiers);

			// start all the copying threads
			for (int i = 0; i < numCopiers; i++) {
				MapOutputCopier copier = new MapOutputCopier(conf, reporter,
						reduceTask.getJobTokenSecret());
				copiers.add(copier);
				copier.start();
			}

			// start the on-disk-merge thread
			if (conf.isPreserve() || conf.isIncrementalStart()
					|| conf.isIncrementalIterative() || conf.isIncrMRBGOnly()) {
				localFSMergerThread = new LocalFSMergeKVSThread(
						(LocalFileSystem) localFileSys);
			} else {
				localFSMergerThread = new LocalFSMergeKVThread(
						(LocalFileSystem) localFileSys);
			}
			localFSMergerThread.start();

			// start the in memory merger thread
			if (conf.isPreserve() || conf.isIncrementalStart()
					|| conf.isIncrementalIterative() || conf.isIncrMRBGOnly()) {
				inMemFSMergeThread = new InMemFSMergeKVSThread();
			} else {
				inMemFSMergeThread = new InMemFSMergeKVThread();
			}
			inMemFSMergeThread.start();

			// start the map events thread
			getMapEventsThread = new GetMapEventsThread();
			getMapEventsThread.start();

			// start the clock for bandwidth measurement
			long startTime = System.currentTimeMillis();
			long currentTime = startTime;
			long lastProgressTime = startTime;
			long lastOutputTime = 0;

			// loop until we get all required outputs
			while (copiedMapOutputs.size() < numMaps && mergeThrowable == null) {

				currentTime = System.currentTimeMillis();
				boolean logNow = false;
				if (currentTime - lastOutputTime > MIN_LOG_TIME) {
					lastOutputTime = currentTime;
					logNow = true;
				}
				if (logNow) {
					LOG.info(reduceTask.getTaskID() + " Need another "
							+ (numMaps - copiedMapOutputs.size())
							+ " map output(s) " + "where " + numInFlight
							+ " is already in progress");
				}

				// Put the hash entries for the failed fetches.
				Iterator<MapOutputLocation> locItr = retryFetches.iterator();

				while (locItr.hasNext()) {
					MapOutputLocation loc = locItr.next();
					List<MapOutputLocation> locList = mapLocations.get(loc
							.getHost());

					// Check if the list exists. Map output location mapping is
					// cleared
					// once the jobtracker restarts and is rebuilt from scratch.
					// Note that map-output-location mapping will be recreated
					// and hence
					// we continue with the hope that we might find some
					// locations
					// from the rebuild map.
					if (locList != null) {
						// Add to the beginning of the list so that this map is
						// tried again before the others and we can hasten the
						// re-execution of this map should there be a problem
						locList.add(0, loc);
					}
				}

				if (retryFetches.size() > 0) {
					LOG.info(reduceTask.getTaskID() + ": " + "Got "
							+ retryFetches.size()
							+ " map-outputs from previous failures");
				}
				// clear the "failed" fetches hashmap
				retryFetches.clear();

				// now walk through the cache and schedule what we can
				int numScheduled = 0;
				int numDups = 0;

				synchronized (scheduledCopies) {

					// Randomize the map output locations to prevent
					// all reduce-tasks swamping the same tasktracker
					List<String> hostList = new ArrayList<String>();
					hostList.addAll(mapLocations.keySet());

					Collections.shuffle(hostList, this.random);

					Iterator<String> hostsItr = hostList.iterator();

					while (hostsItr.hasNext()) {

						String host = hostsItr.next();

						List<MapOutputLocation> knownOutputsByLoc = mapLocations
								.get(host);

						// Check if the list exists. Map output location mapping
						// is
						// cleared once the jobtracker restarts and is rebuilt
						// from
						// scratch.
						// Note that map-output-location mapping will be
						// recreated and
						// hence we continue with the hope that we might find
						// some
						// locations from the rebuild map and add then for
						// fetching.
						if (knownOutputsByLoc == null
								|| knownOutputsByLoc.size() == 0) {
							continue;
						}

						// Identify duplicate hosts here
						if (uniqueHosts.contains(host)) {
							numDups += knownOutputsByLoc.size();
							continue;
						}

						Long penaltyEnd = penaltyBox.get(host);
						boolean penalized = false;

						if (penaltyEnd != null) {
							if (currentTime < penaltyEnd.longValue()) {
								penalized = true;
							} else {
								penaltyBox.remove(host);
							}
						}

						if (penalized)
							continue;

						synchronized (knownOutputsByLoc) {

							locItr = knownOutputsByLoc.iterator();

							while (locItr.hasNext()) {

								MapOutputLocation loc = locItr.next();

								LOG.debug("scheduledCopies is going to add "
										+ loc);

								// Do not schedule fetches from OBSOLETE maps
								if (obsoleteMapIds.contains(loc
										.getTaskAttemptId())) {
									locItr.remove();
									continue;
								}

								uniqueHosts.add(host);
								scheduledCopies.add(loc);
								LOG.debug("scheduledCopies add " + loc);
								locItr.remove(); // remove from knownOutputs
								numInFlight++;
								numScheduled++;

								break; // we have a map from this host
							}
						}
					}
					scheduledCopies.notifyAll();
				}

				if (numScheduled > 0 || logNow) {
					LOG.info(reduceTask.getTaskID() + " Scheduled "
							+ numScheduled + " outputs (" + penaltyBox.size()
							+ " slow hosts and" + numDups + " dup hosts)");
				}

				if (penaltyBox.size() > 0 && logNow) {
					LOG.info("Penalized(slow) Hosts: ");
					for (String host : penaltyBox.keySet()) {
						LOG.info(host + " Will be considered after: "
								+ ((penaltyBox.get(host) - currentTime) / 1000)
								+ " seconds.");
					}
				}

				// if we have no copies in flight and we can't schedule anything
				// new, just wait for a bit
				try {
					if (numInFlight == 0 && numScheduled == 0) {
						// we should indicate progress as we don't want TT to
						// think
						// we're stuck and kill us
						reporter.progress();
						Thread.sleep(5000);
					}
				} catch (InterruptedException e) {
				} // IGNORE

				while (numInFlight > 0 && mergeThrowable == null) {
					// LOG.debug(reduceTask.getTaskID() + " numInFlight = " +
					// numInFlight);

					// the call to getCopyResult will either
					// 1) return immediately with a null or a valid CopyResult
					// object,
					// or
					// 2) if the numInFlight is above maxInFlight, return with a
					// CopyResult object after getting a notification from a
					// fetcher thread,
					// So, when getCopyResult returns null, we can be sure that
					// we aren't busy enough and we should go and get more
					// mapcompletion
					// events from the tasktracker
					CopyResult cr = getCopyResult(numInFlight);

					if (cr == null) {
						break;
					}

					if (cr.getSuccess()) { // a successful copy
						numCopied++;
						lastProgressTime = System.currentTimeMillis();
						reduceShuffleBytes.increment(cr.getSize());

						long secsSinceStart = (System.currentTimeMillis() - startTime) / 1000 + 1;
						float mbs = ((float) reduceShuffleBytes.getCounter())
								/ (1024 * 1024);
						float transferRate = mbs / secsSinceStart;

						copyPhase.startNextPhase();
						copyPhase.setStatus("copy (" + numCopied + " of "
								+ numMaps + " at "
								+ mbpsFormat.format(transferRate) + " MB/s)");

						// Note successful fetch for this mapId to invalidate
						// (possibly) old fetch-failures

						LOG.debug("copy (" + numCopied + " of " + numMaps
								+ " at " + mbpsFormat.format(transferRate)
								+ " MB/s)");
						fetchFailedMaps.remove(cr.getLocation().getTaskId());
					} else if (cr.isObsolete()) {
						// ignore
						LOG.info(reduceTask.getTaskID()
								+ " Ignoring obsolete copy result for Map Task: "
								+ cr.getLocation().getTaskAttemptId()
								+ " from host: " + cr.getHost());
					} else {
						retryFetches.add(cr.getLocation());

						// note the failed-fetch
						TaskAttemptID mapTaskId = cr.getLocation()
								.getTaskAttemptId();
						TaskID mapId = cr.getLocation().getTaskId();

						totalFailures++;
						Integer noFailedFetches = mapTaskToFailedFetchesMap
								.get(mapTaskId);
						noFailedFetches = (noFailedFetches == null) ? 1
								: (noFailedFetches + 1);
						mapTaskToFailedFetchesMap.put(mapTaskId,
								noFailedFetches);
						LOG.info("Task " + getTaskID() + ": Failed fetch #"
								+ noFailedFetches + " from " + mapTaskId);

						if (noFailedFetches >= abortFailureLimit) {
							LOG.fatal(noFailedFetches
									+ " failures downloading " + getTaskID()
									+ ".");
							umbilical.shuffleError(getTaskID(),
									"Exceeded the abort failure limit;"
											+ " bailing-out.", jvmContext);
						}

						checkAndInformJobTracker(
								noFailedFetches,
								mapTaskId,
								cr.getError().equals(
										CopyOutputErrorType.READ_ERROR));

						// note unique failed-fetch maps
						if (noFailedFetches == maxFetchFailuresBeforeReporting) {
							fetchFailedMaps.add(mapId);

							// did we have too many unique failed-fetch maps?
							// and did we fail on too many fetch attempts?
							// and did we progress enough
							// or did we wait for too long without any progress?

							// check if the reducer is healthy
							boolean reducerHealthy = (((float) totalFailures / (totalFailures + numCopied)) < MAX_ALLOWED_FAILED_FETCH_ATTEMPT_PERCENT);

							// check if the reducer has progressed enough
							boolean reducerProgressedEnough = (((float) numCopied / numMaps) >= MIN_REQUIRED_PROGRESS_PERCENT);

							// check if the reducer is stalled for a long time
							// duration for which the reducer is stalled
							int stallDuration = (int) (System
									.currentTimeMillis() - lastProgressTime);
							// duration for which the reducer ran with progress
							int shuffleProgressDuration = (int) (lastProgressTime - startTime);
							// min time the reducer should run without getting
							// killed
							int minShuffleRunDuration = (shuffleProgressDuration > maxMapRuntime) ? shuffleProgressDuration
									: maxMapRuntime;
							boolean reducerStalled = (((float) stallDuration / minShuffleRunDuration) >= MAX_ALLOWED_STALL_TIME_PERCENT);

							// kill if not healthy and has insufficient progress
							if ((fetchFailedMaps.size() >= maxFailedUniqueFetches || fetchFailedMaps
									.size() == (numMaps - copiedMapOutputs
									.size()))
									&& !reducerHealthy
									&& (!reducerProgressedEnough || reducerStalled)) {
								LOG.fatal("Shuffle failed with too many fetch failures "
										+ "and insufficient progress!"
										+ "Killing task " + getTaskID() + ".");
								umbilical.shuffleError(getTaskID(),
										"Exceeded MAX_FAILED_UNIQUE_FETCHES;"
												+ " bailing-out.", jvmContext);
							}

						}

						currentTime = System.currentTimeMillis();
						long currentBackOff = (long) (INITIAL_PENALTY * Math
								.pow(PENALTY_GROWTH_RATE, noFailedFetches));

						penaltyBox.put(cr.getHost(), currentTime
								+ currentBackOff);
						LOG.warn(reduceTask.getTaskID() + " adding host "
								+ cr.getHost()
								+ " to penalty box, next contact in "
								+ (currentBackOff / 1000) + " seconds");
					}
					uniqueHosts.remove(cr.getHost());
					numInFlight--;
				}
			}

			// all done, inform the copiers to exit
			exitGetMapEvents = true;
			try {
				getMapEventsThread.join();
				LOG.info("getMapsEventsThread joined.");
			} catch (InterruptedException ie) {
				LOG.info("getMapsEventsThread threw an exception: "
						+ StringUtils.stringifyException(ie));
			}

			synchronized (copiers) {
				synchronized (scheduledCopies) {
					for (MapOutputCopier copier : copiers) {
						copier.interrupt();
					}
					copiers.clear();
				}
			}

			// copiers are done, exit and notify the waiting merge threads
			synchronized (mapOutputFilesOnDisk) {
				exitLocalFSMerge = true;
				mapOutputFilesOnDisk.notify();
			}

			ramManager.close();

			// Do a merge of in-memory files (if there are any)
			if (mergeThrowable == null) {
				try {
					// Wait for the on-disk merge to complete
					localFSMergerThread.join();
					LOG.info("Interleaved on-disk merge complete: "
							+ mapOutputFilesOnDisk.size() + " files left.");

					// wait for an ongoing merge (if it is in flight) to
					// complete
					inMemFSMergeThread.join();
					LOG.info("In-memory merge complete: "
							+ mapOutputsFilesInMemory.size() + " files left.");
				} catch (InterruptedException ie) {
					LOG.warn(reduceTask.getTaskID()
							+ " Final merge of the inmemory files threw an exception: "
							+ StringUtils.stringifyException(ie));
					// check if the last merge generated an error
					if (mergeThrowable != null) {
						mergeThrowable = ie;
					}
					return false;
				}
			}
			return mergeThrowable == null && copiedMapOutputs.size() == numMaps;
		}

		// Notify the JobTracker
		// after every read error, if 'reportReadErrorImmediately' is true or
		// after every 'maxFetchFailuresBeforeReporting' failures
		protected void checkAndInformJobTracker(int failures,
				TaskAttemptID mapId, boolean readError) {
			if ((reportReadErrorImmediately && readError)
					|| ((failures % maxFetchFailuresBeforeReporting) == 0)) {
				synchronized (ReduceTask.this) {
					taskStatus.addFetchFailedMap(mapId);
					reporter.progress();
					LOG.info("Failed to fetch map-output from "
							+ mapId
							+ " even after MAX_FETCH_RETRIES_PER_MAP retries... "
							+ " or it is a read error, "
							+ " reporting to the JobTracker");
				}
			}
		}

		private long createInMemorySegments(
				List<Segment<K, V>> inMemorySegments, long leaveBytes)
				throws IOException {
			long totalSize = 0L;
			synchronized (mapOutputsFilesInMemory) {
				// fullSize could come from the RamManager, but files can be
				// closed but not yet present in mapOutputsFilesInMemory
				long fullSize = 0L;
				for (MapOutput mo : mapOutputsFilesInMemory) {
					fullSize += mo.data.length;
				}
				while (fullSize > leaveBytes) {
					MapOutput mo = mapOutputsFilesInMemory.remove(0);
					totalSize += mo.data.length;
					fullSize -= mo.data.length;
					Reader<K, V> reader = new InMemoryReader<K, V>(ramManager,
							mo.mapAttemptId, mo.data, 0, mo.data.length);
					Segment<K, V> segment = new Segment<K, V>(reader, true);
					inMemorySegments.add(segment);
				}
			}
			return totalSize;
		}

		private long createInMemorySegments2(
				List<KVSSegment<K, V, SK>> inMemorySegments, long leaveBytes)
				throws IOException {
			long totalSize = 0L;
			synchronized (mapOutputsFilesInMemory) {
				// fullSize could come from the RamManager, but files can be
				// closed but not yet present in mapOutputsFilesInMemory
				long fullSize = 0L;
				for (MapOutput mo : mapOutputsFilesInMemory) {
					fullSize += mo.data.length;
				}
				while (fullSize > leaveBytes) {
					MapOutput mo = mapOutputsFilesInMemory.remove(0);
					totalSize += mo.data.length;
					fullSize -= mo.data.length;

					TrippleReader<K, V, SK> reader = new InMemoryTrippleReader<K, V, SK>(
							ramManager, mo.mapAttemptId, mo.data, 0,
							mo.data.length);

					// LOG.info("add segment " + mo.file + "\t" +
					// mo.mapAttemptId + "\t" + mo.data.length);

					KVSSegment<K, V, SK> segment = new KVSSegment<K, V, SK>(
							reader, true, -1);
					inMemorySegments.add(segment);
				}
			}
			return totalSize;
		}

		/**
		 * Create a RawKeyValueIterator from copied map outputs. All copying
		 * threads have exited, so all of the map outputs are available either
		 * in memory or on disk. We also know that no merges are in progress, so
		 * synchronization is more lax, here.
		 * 
		 * The iterator returned must satisfy the following constraints: 1.
		 * Fewer than io.sort.factor files may be sources 2. No more than
		 * maxInMemReduce bytes of map outputs may be resident in memory when
		 * the reduce begins
		 * 
		 * If we must perform an intermediate merge to satisfy (1), then we can
		 * keep the excluded outputs from (2) in memory and include them in the
		 * first merge pass. If not, then said outputs must be written to disk
		 * first.
		 */
		@SuppressWarnings("unchecked")
		private RawKeyValueIterator createKVIterator(JobConf job,
				FileSystem fs, Reporter reporter) throws IOException {

			// merge config params
			Class<K> keyClass = (Class<K>) job.getMapOutputKeyClass();
			Class<V> valueClass = (Class<V>) job.getMapOutputValueClass();
			boolean keepInputs = job.getKeepFailedTaskFiles();
			final Path tmpDir = new Path(getTaskID().toString());
			final RawComparator<K> comparator = (RawComparator<K>) job
					.getOutputKeyComparator();

			// segments required to vacate memory
			List<Segment<K, V>> memDiskSegments = new ArrayList<Segment<K, V>>();
			long inMemToDiskBytes = 0;
			if (mapOutputsFilesInMemory.size() > 0) {
				TaskID mapId = mapOutputsFilesInMemory.get(0).mapId;
				inMemToDiskBytes = createInMemorySegments(memDiskSegments,
						maxInMemReduce);
				final int numMemDiskSegments = memDiskSegments.size();
				if (numMemDiskSegments > 0
						&& ioSortFactor > mapOutputFilesOnDisk.size()) {
					// must spill to disk, but can't retain in-mem for
					// intermediate merge
					final Path outputPath = mapOutputFile.getInputFileForWrite(
							mapId, inMemToDiskBytes);
					final RawKeyValueIterator rIter = Merger.merge(job, fs,
							keyClass, valueClass, memDiskSegments,
							numMemDiskSegments, tmpDir, comparator, reporter,
							spilledRecordsCounter, null);
					final IFile.Writer writer = new IFile.Writer(job, fs,
							outputPath, keyClass, valueClass, codec, null);
					try {
						Merger.writeFile(rIter, writer, reporter, job);
						addToMapOutputFilesOnDisk(fs.getFileStatus(outputPath));
					} catch (Exception e) {
						if (null != outputPath) {
							fs.delete(outputPath, true);
						}
						throw new IOException("Final merge failed", e);
					} finally {
						if (null != writer) {
							writer.close();
						}
					}
					LOG.info("Merged " + numMemDiskSegments + " segments, "
							+ inMemToDiskBytes + " bytes to disk to satisfy "
							+ "reduce memory limit");
					inMemToDiskBytes = 0;
					memDiskSegments.clear();
				} else if (inMemToDiskBytes != 0) {
					LOG.info("Keeping " + numMemDiskSegments + " segments, "
							+ inMemToDiskBytes + " bytes in memory for "
							+ "intermediate, on-disk merge");
				}
			}

			// segments on disk
			List<Segment<K, V>> diskSegments = new ArrayList<Segment<K, V>>();
			long onDiskBytes = inMemToDiskBytes;
			Path[] onDisk = getMapFiles(fs, false);
			for (Path file : onDisk) {
				onDiskBytes += fs.getFileStatus(file).getLen();
				diskSegments.add(new Segment<K, V>(job, fs, file, codec,
						keepInputs));
			}
			LOG.info("Merging " + onDisk.length + " files, " + onDiskBytes
					+ " bytes from disk");
			Collections.sort(diskSegments, new Comparator<Segment<K, V>>() {
				public int compare(Segment<K, V> o1, Segment<K, V> o2) {
					if (o1.getLength() == o2.getLength()) {
						return 0;
					}
					return o1.getLength() < o2.getLength() ? -1 : 1;
				}
			});

			// build final list of segments from merged backed by disk + in-mem
			List<Segment<K, V>> finalSegments = new ArrayList<Segment<K, V>>();
			long inMemBytes = createInMemorySegments(finalSegments, 0);
			LOG.info("Merging " + finalSegments.size() + " segments, "
					+ inMemBytes + " bytes from memory into reduce");
			if (0 != onDiskBytes) {
				final int numInMemSegments = memDiskSegments.size();
				diskSegments.addAll(0, memDiskSegments);
				memDiskSegments.clear();
				RawKeyValueIterator diskMerge = Merger.merge(job, fs, keyClass,
						valueClass, codec, diskSegments, ioSortFactor,
						numInMemSegments, tmpDir, comparator, reporter, false,
						spilledRecordsCounter, null);
				diskSegments.clear();
				if (0 == finalSegments.size()) {
					return diskMerge;
				}
				finalSegments.add(new Segment<K, V>(new RawKVIteratorReader(
						diskMerge, onDiskBytes), true));
			}
			return Merger.merge(job, fs, keyClass, valueClass, finalSegments,
					finalSegments.size(), tmpDir, comparator, reporter,
					spilledRecordsCounter, null);
		}

		@SuppressWarnings("unchecked")
		private RawKeyValueSourceIterator createKVSIterator(JobConf job,
				FileSystem fs, Reporter reporter) throws IOException {

			// merge config params
			Class<K> keyClass = (Class<K>) job.getMapOutputKeyClass();
			Class<V> valueClass = (Class<V>) job.getMapOutputValueClass();
			Class<SK> skeyClass = (Class<SK>) job.getStaticKeyClass();
			boolean keepInputs = job.getKeepFailedTaskFiles();
			final Path tmpDir = new Path(getTaskID().toString());
			final RawComparator<K> comparator = (RawComparator<K>) job
					.getOutputKeyComparator();
			final RawComparator<SK> comparator2 = (RawComparator<SK>) WritableComparator
					.get(job.getStaticKeyClass().asSubclass(
							WritableComparable.class));

			// segments required to vacate memory
			List<KVSSegment<K, V, SK>> memDiskSegments = new ArrayList<KVSSegment<K, V, SK>>();
			long inMemToDiskBytes = 0;
			if (mapOutputsFilesInMemory.size() > 0) {
				TaskID mapId = mapOutputsFilesInMemory.get(0).mapId;
				inMemToDiskBytes = createInMemorySegments2(memDiskSegments,
						maxInMemReduce);
				final int numMemDiskSegments = memDiskSegments.size();
				if (numMemDiskSegments > 0
						&& ioSortFactor > mapOutputFilesOnDisk.size()) {
					// must spill to disk, but can't retain in-mem for
					// intermediate merge
					final Path outputPath = mapOutputFile.getInputFileForWrite(
							mapId, inMemToDiskBytes);
					final RawKeyValueSourceIterator rIter = PreserveMerger
							.merge(job, fs, keyClass, valueClass, skeyClass,
									memDiskSegments, numMemDiskSegments,
									tmpDir, comparator, comparator2, reporter,
									spilledRecordsCounter, null);
					final TrippleWriter writer = new TrippleWriter(job, fs,
							outputPath, keyClass, valueClass, skeyClass, codec,
							null);
					try {
						PreserveMerger.writeFile(rIter, writer, reporter, job);
						addToMapOutputFilesOnDisk(fs.getFileStatus(outputPath));
					} catch (Exception e) {
						if (null != outputPath) {
							fs.delete(outputPath, true);
						}
						throw new IOException("Final merge failed", e);
					} finally {
						if (null != writer) {
							writer.close();
						}
					}
					LOG.info("Merged " + numMemDiskSegments + " segments, "
							+ inMemToDiskBytes + " bytes to disk to satisfy "
							+ "reduce memory limit");
					inMemToDiskBytes = 0;
					memDiskSegments.clear();
				} else if (inMemToDiskBytes != 0) {
					LOG.info("Keeping " + numMemDiskSegments + " segments, "
							+ inMemToDiskBytes + " bytes in memory for "
							+ "intermediate, on-disk merge");
				}
			}

			// segments on disk
			List<KVSSegment<K, V, SK>> diskSegments = new ArrayList<KVSSegment<K, V, SK>>();
			long onDiskBytes = inMemToDiskBytes;
			Path[] onDisk = getMapFiles(fs, false);
			for (Path file : onDisk) {
				onDiskBytes += fs.getFileStatus(file).getLen();
				diskSegments.add(new KVSSegment<K, V, SK>(job, fs, file, codec,
						keepInputs, -1));
			}

			LOG.info("Merging " + onDisk.length + " files, " + onDiskBytes
					+ " bytes from disk");
			Collections.sort(diskSegments,
					new Comparator<KVSSegment<K, V, SK>>() {
						@Override
						public int compare(KVSSegment<K, V, SK> o1,
								KVSSegment<K, V, SK> o2) {
							if (o1.getLength() == o2.getLength()) {
								return 0;
							}
							return o1.getLength() < o2.getLength() ? -1 : 1;
						}
					});

			// build final list of segments from merged backed by disk + in-mem
			List<KVSSegment<K, V, SK>> finalSegments = new ArrayList<KVSSegment<K, V, SK>>();
			long inMemBytes = createInMemorySegments2(finalSegments, 0);
			LOG.info("Merging " + finalSegments.size() + " segments, "
					+ inMemBytes + " bytes from memory into reduce");
			if (0 != onDiskBytes) {
				final int numInMemSegments = memDiskSegments.size();
				diskSegments.addAll(0, memDiskSegments);
				memDiskSegments.clear();
				RawKeyValueSourceIterator diskMerge = PreserveMerger.merge(job,
						fs, keyClass, valueClass, skeyClass, codec,
						diskSegments, ioSortFactor, numInMemSegments, tmpDir,
						comparator, comparator2, reporter, false,
						spilledRecordsCounter, null);
				diskSegments.clear();
				if (0 == finalSegments.size()) {
					return diskMerge;
				}
				finalSegments.add(new KVSSegment<K, V, SK>(
						new RawKVSIteratorReader(diskMerge, onDiskBytes), true,
						-1));
			}
			return PreserveMerger.merge(job, fs, keyClass, valueClass,
					skeyClass, finalSegments, finalSegments.size(), tmpDir,
					comparator, comparator2, reporter, spilledRecordsCounter,
					null);
		}

		/**
		 * The preserver creation method for multiple preserved files
		 * 
		 * @param job
		 * @param reporter
		 * @return
		 * @throws IOException
		 */
		@SuppressWarnings("unchecked")
		private RawKeyValueSourceIterator createPreserveKVSIterator2(
				JobConf job, int iteration, Reporter reporter)
				throws IOException {

			FileSystem fs = FileSystem.getLocal(job);

			// merge config params
			Class<K> keyClass = (Class<K>) job.getMapOutputKeyClass();
			Class<V> valueClass = (Class<V>) job.getMapOutputValueClass();
			Class<SK> skeyClass = (Class<SK>) job.getStaticKeyClass();
			boolean keepInputs = job.getKeepFailedTaskFiles();
			final Path tmpDir = new Path(getTaskID().toString());
			final RawComparator<K> comparator = (RawComparator<K>) job
					.getOutputKeyComparator();
			final RawComparator<SK> comparator2 = (RawComparator<SK>) WritableComparator
					.get(job.getStaticKeyClass().asSubclass(
							WritableComparable.class));

			if (job.getPreserveStatePath() == null)
				throw new IOException("we need preserved data "
						+ "to perform incremental computation!!!");

			List<KVSSegment<K, V, SK>> preserveSegments = new ArrayList<KVSSegment<K, V, SK>>();

			if (job.isIncrementalStart()) {
				int taskid = reduceTask.getTaskID().getTaskID().getId();
				Path remotePreservedStatePath = new Path(
						job.getPreserveStatePath() + "/reducePreserve-"
								+ taskid);
				Path localPreservedStatePath = new Path(job.get("mapred.incr.tmp", "/mnt/iteroop/")
						+ job.getIterativeAlgorithmID() + "/reducePreserve-"
						+ taskid);

				if (hdfs.exists(remotePreservedStatePath)) {
					// if it doesn't exist the local static file, it means it is
					// the first iteration, so copy it from hdfs
					if (!localfs.exists(localPreservedStatePath)) {
						hdfs.copyToLocalFile(remotePreservedStatePath,
								localPreservedStatePath);
						LOG.info("copy remote preserve file "
								+ remotePreservedStatePath
								+ " to local disk"
								+ localPreservedStatePath
								+ "!!!!!!!!! and file size is "
								+ localfs
										.getFileStatus(localPreservedStatePath)
										.getLen());
					}

					preserveSegments.add(new KVSSegment<K, V, SK>(job, fs,
							localPreservedStatePath, null, true, -1));

				} else {
					throw new IOException(
							"acturally, there is no preserve data on the path you"
									+ " have set! Please check the path and check the map task number "
									+ remotePreservedStatePath);
				}
			} else if (job.isIncrementalIterative() || job.isIncrMRBGOnly()) {
				// the preserve file stored in last snapshot
				int taskid = reduceTask.getTaskID().getTaskID().getId();
				Path remotePreservedStatePath = new Path(
						job.getPreserveStatePath() + "/reducePreserve-"
								+ taskid);
				Path localPreservedStatePath = new Path(job.get("mapred.incr.tmp", "/mnt/iteroop/")
						+ job.getIterativeAlgorithmID() + "/reducePreserve-"
						+ taskid);

				if (hdfs.exists(remotePreservedStatePath)) {
					// if it doesn't exist the local static file, it means it is
					// the first iteration, so copy it from hdfs
					if (!localfs.exists(localPreservedStatePath)) {
						hdfs.copyToLocalFile(remotePreservedStatePath,
								localPreservedStatePath);
						LOG.info("copy remote preserve file "
								+ remotePreservedStatePath + " to local disk"
								+ localPreservedStatePath + "!!!!!!!!!");
					}

					preserveSegments.add(new KVSSegment<K, V, SK>(job, fs,
							localPreservedStatePath, null, true, -1));

				} else {
					throw new IOException(
							"acturally, there is no preserve data on the path you"
									+ " have set! Please check the path and check the map task number "
									+ remotePreservedStatePath);
				}

				// copy the remote reduce preserve file, which are stored in
				// IncrementalSourceValuesIteration.close()
				taskid = reduceTask.getTaskID().getTaskID().getId();
				remotePreservedStatePath = new Path(job.getPreserveStatePath()
						+ "/reducePreserve-Incr-" + (iteration - 1) + "-"
						+ taskid);
				localPreservedStatePath = new Path(job.get("mapred.incr.tmp", "/mnt/iteroop/")
						+ job.getIterativeAlgorithmID()
						+ "/reducePreserve-Incr-" + (iteration - 1) + "-"
						+ taskid);

				// if(hdfs.exists(remotePreservedStatePath)){
				// if it doesn't exist the local static file, it means it is the
				// first iteration, so copy it from hdfs
				if (!localfs.exists(localPreservedStatePath)) {
					hdfs.copyToLocalFile(remotePreservedStatePath,
							localPreservedStatePath);
					LOG.info("copy remote preserve file "
							+ remotePreservedStatePath + " to local disk"
							+ localPreservedStatePath + "!!!!!!!!!");
				}

				preserveSegments.add(new KVSSegment<K, V, SK>(job, fs,
						localPreservedStatePath, null, true, 0));

				// }else{
				// throw new
				// IOException("acturally, there is no preserve data on the path you"
				// +
				// " have set! Please check the path and check the map task number "
				// + remotePreservedStatePath);
				// }
			}

			LOG.info("Merging " + preserveSegments.size() + " preseved files, ");

			return PreserveMerger.merge(job, fs, keyClass, valueClass,
					skeyClass, preserveSegments, preserveSegments.size(),
					tmpDir, comparator, comparator2, reporter,
					spilledRecordsCounter, null);
		}

		@SuppressWarnings("unchecked")
		private RawKeyValueSourceIterator createPreserveKVSIterator(
				JobConf job, int iteration, Reporter reporter)
				throws IOException {

			FileSystem fs = FileSystem.getLocal(job);

			// merge config params
			Class<K> keyClass = (Class<K>) job.getMapOutputKeyClass();
			Class<V> valueClass = (Class<V>) job.getMapOutputValueClass();
			Class<SK> skeyClass = (Class<SK>) job.getStaticKeyClass();
			final Path tmpDir = new Path(getTaskID().toString());
			final RawComparator<K> comparator = (RawComparator<K>) job
					.getOutputKeyComparator();
			final RawComparator<SK> comparator2 = (RawComparator<SK>) WritableComparator
					.get(job.getStaticKeyClass().asSubclass(
							WritableComparable.class));

			if (job.getPreserveStatePath() == null)
				throw new IOException("we need preserved data "
						+ "to perform incremental computation!!!");

			List<KVSSegment<K, V, SK>> preserveSegments = new ArrayList<KVSSegment<K, V, SK>>();

			if (job.isIncrementalStart()) {
				int taskid = reduceTask.getTaskID().getTaskID().getId();
				Path remotePreservedStatePath = new Path(
						job.getPreserveStatePath() + "/preserve-" + taskid);
				Path localPreservedStatePath = new Path(job.get("mapred.incr.tmp", "/mnt/iteroop/")
						+ job.getIterativeAlgorithmID() + "/preserve-" + taskid);

				if (hdfs.exists(remotePreservedStatePath)) {
					// if it doesn't exist the local static file, it means it is
					// the first iteration, so copy it from hdfs
					if (!localfs.exists(localPreservedStatePath)) {
						hdfs.copyToLocalFile(remotePreservedStatePath,
								localPreservedStatePath);
						LOG.info("copy remote preserve file "
								+ remotePreservedStatePath
								+ " to local disk"
								+ localPreservedStatePath
								+ "!!!!!!!!! and file size is "
								+ localfs
										.getFileStatus(localPreservedStatePath)
										.getLen());
					}

					preserveSegments.add(new KVSSegment<K, V, SK>(job, fs,
							localPreservedStatePath, null, true, 0));

				} else {
					throw new IOException(
							"acturally, there is no preserve data on the path you"
									+ " have set! Please check the path and check the map task number "
									+ remotePreservedStatePath);
				}

				Path remoteIndexPreservedStatePath = new Path(
						job.getPreserveStatePath() + "/preserve-" + taskid
								+ ".index");
				Path localIndexPreservedStatePath = new Path(job.get("mapred.incr.tmp", "/mnt/iteroop/")
						+ job.getIterativeAlgorithmID() + "/preserve-" + taskid
						+ ".index");

				if (hdfs.exists(remoteIndexPreservedStatePath)) {
					// if it doesn't exist the local static file, it means it is
					// the first iteration, so copy it from hdfs
					if (!localfs.exists(localIndexPreservedStatePath)) {
						hdfs.copyToLocalFile(remoteIndexPreservedStatePath,
								localIndexPreservedStatePath);
						LOG.info("copy remote preserve file "
								+ remoteIndexPreservedStatePath
								+ " to local disk"
								+ localIndexPreservedStatePath
								+ "!!!!!!!!! and file size is "
								+ localfs.getFileStatus(
										localIndexPreservedStatePath).getLen());
					}

					// use RandomAccessKSVFile
					// adsfsd.add(new KVSSegment<K,V,SK>(job, fs,
					// localIndexPreservedStatePath, null, true, 0));

				} else {
					throw new IOException(
							"acturally, there is no preserve data on the path you"
									+ " have set! Please check the path and check the map task number "
									+ remoteIndexPreservedStatePath);
				}
			} else if (job.isIncrementalIterative() || job.isIncrMRBGOnly()) {
				// the preserve file stored in last snapshot
				taskid = reduceTask.getTaskID().getTaskID().getId();
				Path remotePreservedStatePath = new Path(
						job.getPreserveStatePath() + "/reducePreserve-Incr-"
								+ (iteration - 1) + "-" + taskid);
				Path localPreservedStatePath = new Path(job.get("mapred.incr.tmp", "/mnt/iteroop/")
						+ job.getIterativeAlgorithmID()
						+ "/reducePreserve-Incr-" + (iteration - 1) + "-"
						+ taskid);

				if (!localfs.exists(localPreservedStatePath)) {
					hdfs.copyToLocalFile(remotePreservedStatePath,
							localPreservedStatePath);
					LOG.info("copy remote preserve file "
							+ remotePreservedStatePath + " to local disk"
							+ localPreservedStatePath + "!!!!!!!!!");
				}

				preserveSegments.add(new KVSSegment<K, V, SK>(job, fs,
						localPreservedStatePath, null, true, 0));
			}

			LOG.info("Merging " + preserveSegments.size() + " preseved files, ");

			return PreserveMerger.merge(job, fs, keyClass, valueClass,
					skeyClass, preserveSegments, preserveSegments.size(),
					tmpDir, comparator, comparator2, reporter,
					spilledRecordsCounter, null);
		}

		class RawKVIteratorReader extends IFile.Reader<K, V> {

			private final RawKeyValueIterator kvIter;

			public RawKVIteratorReader(RawKeyValueIterator kvIter, long size)
					throws IOException {
				super(null, null, size, null, spilledRecordsCounter);
				this.kvIter = kvIter;
			}

			public boolean next(DataInputBuffer key, DataInputBuffer value)
					throws IOException {
				if (kvIter.next()) {
					final DataInputBuffer kb = kvIter.getKey();
					final DataInputBuffer vb = kvIter.getValue();
					final int kp = kb.getPosition();
					final int klen = kb.getLength() - kp;
					key.reset(kb.getData(), kp, klen);
					final int vp = vb.getPosition();
					final int vlen = vb.getLength() - vp;
					value.reset(vb.getData(), vp, vlen);
					bytesRead += klen + vlen;
					return true;
				}
				return false;
			}

			public long getPosition() throws IOException {
				return bytesRead;
			}

			public void close() throws IOException {
				kvIter.close();
			}
		}

		class RawKVSIteratorReader extends IFile.TrippleReader<K, V, SK> {

			private final RawKeyValueSourceIterator kvsIter;

			public RawKVSIteratorReader(RawKeyValueSourceIterator kvsIter,
					long size) throws IOException {
				super(null, null, size, null, spilledRecordsCounter);
				this.kvsIter = kvsIter;
			}

			public boolean next(DataInputBuffer key, DataInputBuffer value,
					DataInputBuffer skey) throws IOException {
				if (kvsIter.next()) {
					final DataInputBuffer kb = kvsIter.getKey();
					final DataInputBuffer vb = kvsIter.getValue();
					final DataInputBuffer sb = kvsIter.getSKey();
					final int kp = kb.getPosition();
					final int klen = kb.getLength() - kp;
					key.reset(kb.getData(), kp, klen);
					final int vp = vb.getPosition();
					final int vlen = vb.getLength() - vp;
					value.reset(vb.getData(), vp, vlen);
					final int sp = sb.getPosition();
					final int slen = sb.getLength() - sp;
					skey.reset(sb.getData(), sp, slen);
					bytesRead += klen + vlen + slen;
					return true;
				}
				return false;
			}

			public long getPosition() throws IOException {
				return bytesRead;
			}

			public void close() throws IOException {
				kvsIter.close();
			}
		}

		private CopyResult getCopyResult(int numInFlight) {
			synchronized (copyResults) {
				while (copyResults.isEmpty()) {
					try {
						// The idea is that if we have scheduled enough, we can
						// wait until
						// we hear from one of the copiers.
						if (busyEnough(numInFlight)) {
							copyResults.wait();
						} else {
							return null;
						}
					} catch (InterruptedException e) {
					}
				}
				return copyResults.remove(0);
			}
		}

		private void addToMapOutputFilesOnDisk(FileStatus status) {
			synchronized (mapOutputFilesOnDisk) {
				mapOutputFilesOnDisk.add(status);
				mapOutputFilesOnDisk.notify();
			}
		}

		private abstract class LocalFSMerger extends Thread {
		}

		/**
		 * Starts merging the local copy (on disk) of the map's output so that
		 * most of the reducer's input is sorted i.e overlapping shuffle and
		 * merge phases.
		 */
		private class LocalFSMergeKVThread extends LocalFSMerger {
			private LocalFileSystem localFileSys;

			public LocalFSMergeKVThread(LocalFileSystem fs) {
				this.localFileSys = fs;
				setName("Thread for merging on-disk files");
				setDaemon(true);
			}

			@SuppressWarnings("unchecked")
			public void run() {
				try {
					LOG.info(reduceTask.getTaskID() + " Thread started: "
							+ getName());
					while (!exitLocalFSMerge) {
						synchronized (mapOutputFilesOnDisk) {
							while (!exitLocalFSMerge
									&& mapOutputFilesOnDisk.size() < (2 * ioSortFactor - 1)) {
								LOG.info(reduceTask.getTaskID()
										+ " Thread waiting: " + getName());
								mapOutputFilesOnDisk.wait();
							}
						}
						if (exitLocalFSMerge) {// to avoid running one extra
												// time in the end
							break;
						}
						List<Path> mapFiles = new ArrayList<Path>();
						long approxOutputSize = 0;
						int bytesPerSum = reduceTask.getConf().getInt(
								"io.bytes.per.checksum", 512);
						LOG.info(reduceTask.getTaskID() + "We have  "
								+ mapOutputFilesOnDisk.size()
								+ " map outputs on disk. "
								+ "Triggering merge of " + ioSortFactor
								+ " files");
						// 1. Prepare the list of files to be merged. This list
						// is prepared
						// using a list of map output files on disk. Currently
						// we merge
						// io.sort.factor files into 1.
						synchronized (mapOutputFilesOnDisk) {
							for (int i = 0; i < ioSortFactor; ++i) {
								FileStatus filestatus = mapOutputFilesOnDisk
										.first();
								mapOutputFilesOnDisk.remove(filestatus);
								mapFiles.add(filestatus.getPath());
								approxOutputSize += filestatus.getLen();
							}
						}

						// sanity check
						if (mapFiles.size() == 0) {
							return;
						}

						// add the checksum length
						approxOutputSize += ChecksumFileSystem
								.getChecksumLength(approxOutputSize,
										bytesPerSum);

						// 2. Start the on-disk merge process
						Path outputPath = lDirAlloc.getLocalPathForWrite(
								mapFiles.get(0).toString(), approxOutputSize,
								conf).suffix(".merged");
						IFile.Writer writer = new IFile.Writer(conf, rfs,
								outputPath, conf.getMapOutputKeyClass(),
								conf.getMapOutputValueClass(), codec, null);
						RawKeyValueIterator iter = null;
						Path tmpDir = new Path(reduceTask.getTaskID()
								.toString());
						try {
							iter = Merger
									.merge(conf, rfs, conf
											.getMapOutputKeyClass(), conf
											.getMapOutputValueClass(), codec,
											mapFiles.toArray(new Path[mapFiles
													.size()]), true,
											ioSortFactor, tmpDir, conf
													.getOutputKeyComparator(),
											reporter, spilledRecordsCounter,
											null);

							Merger.writeFile(iter, writer, reporter, conf);
							writer.close();
						} catch (Exception e) {
							localFileSys.delete(outputPath, true);
							throw new IOException(
									StringUtils.stringifyException(e));
						}

						synchronized (mapOutputFilesOnDisk) {
							addToMapOutputFilesOnDisk(localFileSys
									.getFileStatus(outputPath));
						}

						LOG.info(reduceTask.getTaskID()
								+ " Finished merging "
								+ mapFiles.size()
								+ " map output files on disk of total-size "
								+ approxOutputSize
								+ "."
								+ " Local output file is "
								+ outputPath
								+ " of size "
								+ localFileSys.getFileStatus(outputPath)
										.getLen());
					}
				} catch (Exception e) {
					LOG.warn(reduceTask.getTaskID()
							+ " Merging of the local FS files threw an exception: "
							+ StringUtils.stringifyException(e));
					if (mergeThrowable == null) {
						mergeThrowable = e;
					}
				} catch (Throwable t) {
					String msg = getTaskID()
							+ " : Failed to merge on the local FS"
							+ StringUtils.stringifyException(t);
					reportFatalError(getTaskID(), t, msg);
				}
			}
		}

		private class LocalFSMergeKVSThread extends LocalFSMerger {
			private LocalFileSystem localFileSys;

			public LocalFSMergeKVSThread(LocalFileSystem fs) {
				this.localFileSys = fs;
				setName("Thread for merging on-disk preserve files");
				setDaemon(true);
			}

			@SuppressWarnings("unchecked")
			public void run() {
				try {
					LOG.info(reduceTask.getTaskID() + " Thread started: "
							+ getName());
					while (!exitLocalFSMerge) {
						synchronized (mapOutputFilesOnDisk) {
							while (!exitLocalFSMerge
									&& mapOutputFilesOnDisk.size() < (2 * ioSortFactor - 1)) {
								LOG.info(reduceTask.getTaskID()
										+ " Thread waiting: " + getName());
								mapOutputFilesOnDisk.wait();
							}
						}
						if (exitLocalFSMerge) {// to avoid running one extra
												// time in the end
							break;
						}
						List<Path> mapFiles = new ArrayList<Path>();
						long approxOutputSize = 0;
						int bytesPerSum = reduceTask.getConf().getInt(
								"io.bytes.per.checksum", 512);
						LOG.info(reduceTask.getTaskID() + "We have  "
								+ mapOutputFilesOnDisk.size()
								+ " map outputs on disk. "
								+ "Triggering merge of " + ioSortFactor
								+ " files");
						// 1. Prepare the list of files to be merged. This list
						// is prepared
						// using a list of map output files on disk. Currently
						// we merge
						// io.sort.factor files into 1.
						synchronized (mapOutputFilesOnDisk) {
							for (int i = 0; i < ioSortFactor; ++i) {
								FileStatus filestatus = mapOutputFilesOnDisk
										.first();
								mapOutputFilesOnDisk.remove(filestatus);
								mapFiles.add(filestatus.getPath());
								approxOutputSize += filestatus.getLen();
							}
						}

						// sanity check
						if (mapFiles.size() == 0) {
							return;
						}

						// add the checksum length
						approxOutputSize += ChecksumFileSystem
								.getChecksumLength(approxOutputSize,
										bytesPerSum);

						// 2. Start the on-disk merge process
						Path outputPath = lDirAlloc.getLocalPathForWrite(
								mapFiles.get(0).toString(), approxOutputSize,
								conf).suffix(".merged");
						IFile.TrippleWriter writer = new IFile.TrippleWriter(
								conf, rfs, outputPath,
								conf.getMapOutputKeyClass(),
								conf.getMapOutputValueClass(),
								conf.getStaticKeyClass(), codec, null);
						RawKeyValueSourceIterator iter = null;
						final RawComparator<SK> comparator2 = (RawComparator<SK>) WritableComparator
								.get(conf.getStaticKeyClass().asSubclass(
										WritableComparable.class));

						Path tmpDir = new Path(reduceTask.getTaskID()
								.toString());
						try {
							iter = PreserveMerger
									.merge(conf, rfs, (Class<K>) conf
											.getMapOutputKeyClass(),
											(Class<V>) conf
													.getMapOutputValueClass(),
											(Class<SK>) conf
													.getStaticKeyClass(),
											codec, mapFiles
													.toArray(new Path[mapFiles
															.size()]), true,
											ioSortFactor, tmpDir, conf
													.getOutputKeyComparator(),
											comparator2, reporter,
											spilledRecordsCounter, null);

							PreserveMerger.writeFile(iter, writer, reporter,
									conf);
							writer.close();
						} catch (Exception e) {
							localFileSys.delete(outputPath, true);
							throw new IOException(
									StringUtils.stringifyException(e));
						}

						synchronized (mapOutputFilesOnDisk) {
							addToMapOutputFilesOnDisk(localFileSys
									.getFileStatus(outputPath));
						}

						LOG.info(reduceTask.getTaskID()
								+ " Finished merging "
								+ mapFiles.size()
								+ " map output files on disk of total-size "
								+ approxOutputSize
								+ "."
								+ " Local output file is "
								+ outputPath
								+ " of size "
								+ localFileSys.getFileStatus(outputPath)
										.getLen());
					}
				} catch (Exception e) {
					LOG.warn(reduceTask.getTaskID()
							+ " Merging of the local FS files threw an exception: "
							+ StringUtils.stringifyException(e));
					if (mergeThrowable == null) {
						mergeThrowable = e;
					}
				} catch (Throwable t) {
					String msg = getTaskID()
							+ " : Failed to merge on the local FS"
							+ StringUtils.stringifyException(t);
					reportFatalError(getTaskID(), t, msg);
				}
			}
		}

		private abstract class InMemFSMergeThread extends Thread {
		}

		private class InMemFSMergeKVThread extends InMemFSMergeThread {

			public InMemFSMergeKVThread() {
				setName("Thread for merging in memory files");
				setDaemon(true);
			}

			public void run() {
				LOG.info(reduceTask.getTaskID() + " Thread started: "
						+ getName());
				try {
					boolean exit = false;
					do {
						exit = ramManager.waitForDataToMerge();
						if (!exit) {
							doInMemMerge();
						}
					} while (!exit);
				} catch (Exception e) {
					LOG.warn(reduceTask.getTaskID()
							+ " Merge of the inmemory files threw an exception: "
							+ StringUtils.stringifyException(e));
					ReduceCopier.this.mergeThrowable = e;
				} catch (Throwable t) {
					String msg = getTaskID() + " : Failed to merge in memory"
							+ StringUtils.stringifyException(t);
					reportFatalError(getTaskID(), t, msg);
				}
			}

			@SuppressWarnings("unchecked")
			private void doInMemMerge() throws IOException {
				if (mapOutputsFilesInMemory.size() == 0) {
					return;
				}

				// name this output file same as the name of the first file that
				// is
				// there in the current list of inmem files (this is guaranteed
				// to
				// be absent on the disk currently. So we don't overwrite a
				// prev.
				// created spill). Also we need to create the output file now
				// since
				// it is not guaranteed that this file will be present after
				// merge
				// is called (we delete empty files as soon as we see them
				// in the merge method)

				// figure out the mapId
				TaskID mapId = mapOutputsFilesInMemory.get(0).mapId;

				List<Segment<K, V>> inMemorySegments = new ArrayList<Segment<K, V>>();
				long mergeOutputSize = createInMemorySegments(inMemorySegments,
						0);
				int noInMemorySegments = inMemorySegments.size();

				Path outputPath = mapOutputFile.getInputFileForWrite(mapId,
						mergeOutputSize);

				IFile.Writer writer = new IFile.Writer(conf, rfs, outputPath,
						conf.getMapOutputKeyClass(),
						conf.getMapOutputValueClass(), codec, null);

				RawKeyValueIterator rIter = null;
				try {
					LOG.info("Initiating in-memory merge with "
							+ noInMemorySegments + " segments...");

					rIter = Merger.merge(conf, rfs,
							(Class<K>) conf.getMapOutputKeyClass(),
							(Class<V>) conf.getMapOutputValueClass(),
							inMemorySegments, inMemorySegments.size(),
							new Path(reduceTask.getTaskID().toString()),
							conf.getOutputKeyComparator(), reporter,
							spilledRecordsCounter, null);

					if (combinerRunner == null) {
						Merger.writeFile(rIter, writer, reporter, conf);
					} else {
						combineCollector.setWriter(writer);
						combinerRunner.combine(rIter, combineCollector);
					}
					writer.close();

					LOG.info(reduceTask.getTaskID() + " Merge of the "
							+ noInMemorySegments + " files in-memory complete."
							+ " Local file is " + outputPath + " of size "
							+ localFileSys.getFileStatus(outputPath).getLen());
				} catch (Exception e) {
					// make sure that we delete the ondisk file that we created
					// earlier when we invoked cloneFileAttributes
					localFileSys.delete(outputPath, true);
					throw (IOException) new IOException(
							"Intermediate merge failed").initCause(e);
				}

				// Note the output of the merge
				FileStatus status = localFileSys.getFileStatus(outputPath);
				synchronized (mapOutputFilesOnDisk) {
					addToMapOutputFilesOnDisk(status);
				}
			}
		}

		private class InMemFSMergeKVSThread extends InMemFSMergeThread {

			public InMemFSMergeKVSThread() {
				setName("Thread for merging in memory preserve files");
				setDaemon(true);
			}

			public void run() {
				LOG.info(reduceTask.getTaskID() + " Thread started: "
						+ getName());
				try {
					boolean exit = false;
					do {
						exit = ramManager.waitForDataToMerge();
						if (!exit) {
							doInMemMerge();
						}
					} while (!exit);
				} catch (Exception e) {
					LOG.warn(reduceTask.getTaskID()
							+ " Merge of the inmemory files threw an exception: "
							+ StringUtils.stringifyException(e));
					ReduceCopier.this.mergeThrowable = e;
				} catch (Throwable t) {
					String msg = getTaskID() + " : Failed to merge in memory"
							+ StringUtils.stringifyException(t);
					reportFatalError(getTaskID(), t, msg);
				}
			}

			@SuppressWarnings("unchecked")
			private void doInMemMerge() throws IOException {
				if (mapOutputsFilesInMemory.size() == 0) {
					return;
				}

				// name this output file same as the name of the first file that
				// is
				// there in the current list of inmem files (this is guaranteed
				// to
				// be absent on the disk currently. So we don't overwrite a
				// prev.
				// created spill). Also we need to create the output file now
				// since
				// it is not guaranteed that this file will be present after
				// merge
				// is called (we delete empty files as soon as we see them
				// in the merge method)

				// figure out the mapId
				TaskID mapId = mapOutputsFilesInMemory.get(0).mapId;

				List<KVSSegment<K, V, SK>> inMemorySegments = new ArrayList<KVSSegment<K, V, SK>>();
				long mergeOutputSize = createInMemorySegments2(
						inMemorySegments, 0);
				int noInMemorySegments = inMemorySegments.size();

				Path outputPath = mapOutputFile.getInputFileForWrite(mapId,
						mergeOutputSize);

				IFile.TrippleWriter writer = new IFile.TrippleWriter(conf, rfs,
						outputPath, conf.getMapOutputKeyClass(),
						conf.getMapOutputValueClass(),
						conf.getStaticKeyClass(), codec, null);

				final RawComparator<SK> comparator2 = (RawComparator<SK>) WritableComparator
						.get(conf.getStaticKeyClass().asSubclass(
								WritableComparable.class));

				RawKeyValueSourceIterator rIter = null;
				try {
					LOG.info("Initiating in-memory merge with "
							+ noInMemorySegments + " segments...");

					rIter = PreserveMerger.merge(conf, rfs,
							(Class<K>) conf.getMapOutputKeyClass(),
							(Class<V>) conf.getMapOutputValueClass(),
							(Class<SK>) conf.getStaticKeyClass(),
							inMemorySegments, inMemorySegments.size(),
							new Path(reduceTask.getTaskID().toString()),
							conf.getOutputKeyComparator(), comparator2,
							reporter, spilledRecordsCounter, null);

					if (combinerRunner == null) {
						PreserveMerger.writeFile(rIter, writer, reporter, conf);
					} else {
						throw new RuntimeException(
								"Currently, we don't support combiner!");
						// combineCollector.setWriter(writer);
						// combinerRunner.combine(rIter, combineCollector);
					}
					writer.close();

					LOG.info(reduceTask.getTaskID() + " Merge of the "
							+ noInMemorySegments
							+ " preserve files in-memory complete."
							+ " Local file is " + outputPath + " of size "
							+ localFileSys.getFileStatus(outputPath).getLen());
				} catch (Exception e) {
					// make sure that we delete the ondisk file that we created
					// earlier when we invoked cloneFileAttributes
					localFileSys.delete(outputPath, true);
					throw (IOException) new IOException(
							"Intermediate merge failed").initCause(e);
				}

				// Note the output of the merge
				FileStatus status = localFileSys.getFileStatus(outputPath);
				synchronized (mapOutputFilesOnDisk) {
					addToMapOutputFilesOnDisk(status);
				}
			}
		}

		private class GetMapEventsThread extends Thread {

			private IntWritable fromEventId = new IntWritable(0);
			private static final long SLEEP_TIME = 1000;

			public GetMapEventsThread() {
				setName("Thread for polling Map Completion Events");
				setDaemon(true);
			}

			@Override
			public void run() {

				LOG.info(reduceTask.getTaskID() + " Thread started: "
						+ getName());

				do {
					try {
						if (mapLocations.size() >= numMaps) {
							continue;
						}
						int numNewMaps = getMapCompletionEvents();
						if (LOG.isDebugEnabled()) {
							if (numNewMaps > 0) {
								LOG.debug(reduceTask.getTaskID() + ": "
										+ "Got " + numNewMaps
										+ " new map-outputs");
							}
						}
						Thread.sleep(SLEEP_TIME);
					} catch (InterruptedException e) {
						LOG.warn(reduceTask.getTaskID()
								+ " GetMapEventsThread returning after an "
								+ " interrupted exception");
						return;
					} catch (Throwable t) {
						String msg = reduceTask.getTaskID()
								+ " GetMapEventsThread Ignoring exception : "
								+ StringUtils.stringifyException(t);
						reportFatalError(getTaskID(), t, msg);
					}
				} while (!exitGetMapEvents);

				LOG.info("GetMapEventsThread exiting");

			}

			/**
			 * Queries the {@link TaskTracker} for a set of map-completion
			 * events from a given event ID.
			 * 
			 * @throws IOException
			 */
			private int getMapCompletionEvents() throws IOException {

				int numNewMaps = 0;

				MapTaskCompletionEventsUpdate update = umbilical
						.getMapCompletionEvents(reduceTask.getJobID(),
								fromEventId.get(), MAX_EVENTS_TO_FETCH,
								reduceTask.getTaskID(), jvmContext);
				TaskCompletionEvent events[] = update
						.getMapTaskCompletionEvents();

				// Check if the reset is required.
				// Since there is no ordering of the task completion events at
				// the
				// reducer, the only option to sync with the new jobtracker is
				// to reset
				// the events index
				if (update.shouldReset()) {
					fromEventId.set(0);
					obsoleteMapIds.clear(); // clear the obsolete map
					mapLocations.clear(); // clear the map locations mapping
				}

				// Update the last seen event ID
				fromEventId.set(fromEventId.get() + events.length);

				// Process the TaskCompletionEvents:
				// 1. Save the SUCCEEDED maps in knownOutputs to fetch the
				// outputs.
				// 2. Save the OBSOLETE/FAILED/KILLED maps in obsoleteOutputs to
				// stop
				// fetching from those maps.
				// 3. Remove TIPFAILED maps from neededOutputs since we don't
				// need their
				// outputs at all.
				for (TaskCompletionEvent event : events) {
					switch (event.getTaskStatus()) {
					case SUCCEEDED: {
						// LOG.info("SUCCEEDED event is received!");
						URI u = URI.create(event.getTaskTrackerHttp());
						String host = u.getHost();
						TaskAttemptID taskId = event.getTaskAttemptId();
						URL mapOutputLocation = new URL(
								event.getTaskTrackerHttp() + "/mapOutput?job="
										+ taskId.getJobID() + "&map=" + taskId
										+ "&reduce=" + getPartition());
						List<MapOutputLocation> loc = mapLocations.get(host);
						if (loc == null) {
							loc = Collections
									.synchronizedList(new LinkedList<MapOutputLocation>());
							mapLocations.put(host, loc);
						}
						loc.add(new MapOutputLocation(taskId, host,
								mapOutputLocation));

						LOG.info("put loc " + host + "\t" + mapOutputLocation);

						numNewMaps++;
					}
						break;
					case RUNNING: {
						// LOG.info("RUNNING event is received!");
						URI u = URI.create(event.getTaskTrackerHttp());
						String host = u.getHost();
						TaskAttemptID taskId = event.getTaskAttemptId();
						URL mapOutputLocation = new URL(
								event.getTaskTrackerHttp() + "/mapOutput?job="
										+ taskId.getJobID() + "&map=" + taskId
										+ "&reduce=" + getPartition());
						List<MapOutputLocation> loc = mapLocations.get(host);
						if (loc == null) {
							loc = Collections
									.synchronizedList(new LinkedList<MapOutputLocation>());
							mapLocations.put(host, loc);
						}
						loc.add(new MapOutputLocation(taskId, host,
								mapOutputLocation));
						numNewMaps++;
					}
						break;
					case FAILED:
					case KILLED:
					case OBSOLETE: {
						obsoleteMapIds.add(event.getTaskAttemptId());
						LOG.info("Ignoring obsolete output of "
								+ event.getTaskStatus() + " map-task: '"
								+ event.getTaskAttemptId() + "'");
					}
						break;
					case TIPFAILED: {
						copiedMapOutputs.add(event.getTaskAttemptId()
								.getTaskID());
						LOG.info("Ignoring output of failed map TIP: '"
								+ event.getTaskAttemptId() + "'");
					}
						break;
					}
				}
				return numNewMaps;
			}
		}
	}

	/**
	 * Return the exponent of the power of two closest to the given positive
	 * value, or zero if value leq 0. This follows the observation that the msb
	 * of a given value is also the closest power of two, unless the bit
	 * following it is set.
	 */
	private static int getClosestPowerOf2(int value) {
		if (value <= 0)
			throw new IllegalArgumentException("Undefined for " + value);
		final int hob = Integer.highestOneBit(value);
		return Integer.numberOfTrailingZeros(hob)
				+ (((hob >>> 1) & value) == 0 ? 0 : 1);
	}
}
