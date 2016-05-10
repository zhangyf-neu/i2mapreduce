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

import static org.apache.hadoop.mapred.Task.Counter.COMBINE_INPUT_RECORDS;
import static org.apache.hadoop.mapred.Task.Counter.COMBINE_OUTPUT_RECORDS;
import static org.apache.hadoop.mapred.Task.Counter.MAP_INPUT_BYTES;
import static org.apache.hadoop.mapred.Task.Counter.MAP_INPUT_RECORDS;
import static org.apache.hadoop.mapred.Task.Counter.MAP_OUTPUT_BYTES;
import static org.apache.hadoop.mapred.Task.Counter.MAP_OUTPUT_MATERIALIZED_BYTES;
import static org.apache.hadoop.mapred.Task.Counter.MAP_OUTPUT_RECORDS;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.IFile.TrippleWriter;
import org.apache.hadoop.mapred.IFile.Writer;
import org.apache.hadoop.mapred.Merger.Segment;
import org.apache.hadoop.mapred.PreserveMerger.KVSSegment;
import org.apache.hadoop.mapred.SortedRanges.SkipRangeIterator;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitIndex;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.IndexedSorter;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.QuickSort;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

/** A Map task. */
class MapTask_backup extends Task {
  /**
   * The size of each record in the index file for the map-outputs.
   */
  public static final int MAP_OUTPUT_INDEX_RECORD_LENGTH = 24;

  private TaskSplitIndex splitMetaInfo = new TaskSplitIndex();
  private final static int APPROX_HEADER_LENGTH = 150;

  private static final Log LOG = LogFactory.getLog(MapTask.class.getName());

  {   // set phase for this task
    setPhase(TaskStatus.Phase.MAP); 
  }

  public MapTask_backup() {
    super();
  }

  public MapTask_backup(String jobFile, TaskAttemptID taskId, 
                 int partition, TaskSplitIndex splitIndex,
                 int numSlotsRequired) {
    super(jobFile, taskId, partition, numSlotsRequired);
    this.splitMetaInfo = splitIndex;
  }

  @Override
  public boolean isMapTask() {
    return true;
  }

  @Override
  public void localizeConfiguration(JobConf conf)
      throws IOException {
    super.localizeConfiguration(conf);
    // split.info file is used only by IsolationRunner.
    // Write the split file to the local disk if it is a normal map task (not a
    // job-setup or a job-cleanup task) and if the user wishes to run
    // IsolationRunner either by setting keep.failed.tasks.files to true or by
    // using keep.tasks.files.pattern
    if (supportIsolationRunner(conf) && isMapOrReduce()) {
      // localize the split meta-information
      Path localSplitMeta =
        new LocalDirAllocator("mapred.local.dir").getLocalPathForWrite(
            TaskTracker.getLocalSplitFile(conf.getUser(), getJobID()
                .toString(), getTaskID().toString()), conf);
      LOG.debug("Writing local split to " + localSplitMeta);
      DataOutputStream out = FileSystem.getLocal(conf).create(localSplitMeta);
      splitMetaInfo.write(out);
      out.close();
    }
  }
  
  @Override
  public TaskRunner createRunner(TaskTracker tracker, 
                                 TaskTracker.TaskInProgress tip,
                                 TaskTracker.RunningJob rjob
                                 ) throws IOException {
    return new MapTaskRunner(tip, tracker, this.conf, rjob);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    if (isMapOrReduce()) {
      if (splitMetaInfo != null) {
        splitMetaInfo.write(out);
      } else {
        new TaskSplitIndex().write(out);
      }
      //TODO do we really need to set this to null?
      splitMetaInfo = null;
    }
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    if (isMapOrReduce()) {
      splitMetaInfo.readFields(in);
    }
  }

  /**
   * This class wraps the user's record reader to update the counters and
   * progress as records are read.
   * @param <K>
   * @param <V>
   */
  class TrackedRecordReader<K, V> 
      implements RecordReader<K,V> {
    private RecordReader<K,V> rawIn;
    private Counters.Counter inputByteCounter;
    private Counters.Counter inputRecordCounter;
    private Counters.Counter fileInputByteCounter;
    private InputSplit split;
    private TaskReporter reporter;
    private long beforePos = -1;
    private long afterPos = -1;
    private long bytesInPrev = -1;
    private long bytesInCurr = -1;
    private final Statistics fsStats;

    TrackedRecordReader(InputSplit split, JobConf job, TaskReporter reporter)
        throws IOException {
      inputRecordCounter = reporter.getCounter(MAP_INPUT_RECORDS);
      inputByteCounter = reporter.getCounter(MAP_INPUT_BYTES);
      fileInputByteCounter = reporter
          .getCounter(FileInputFormat.Counter.BYTES_READ);

      Statistics matchedStats = null;
      if (split instanceof FileSplit) {
        matchedStats = getFsStatistics(((FileSplit) split).getPath(), job);
      } 
      fsStats = matchedStats;
      
      bytesInPrev = getInputBytes(fsStats);
      rawIn = job.getInputFormat().getRecordReader(split, job, reporter);
      bytesInCurr = getInputBytes(fsStats);
      fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
      
      this.reporter = reporter;
      this.split = split;
      conf = job;
    }

    public K createKey() {
      return rawIn.createKey();
    }
      
    public V createValue() {
      return rawIn.createValue();
    }
     
    public synchronized boolean next(K key, V value)
    throws IOException {
      boolean ret = moveToNext(key, value);      
      if (ret) {
        incrCounters();
      }
      return ret;
    }
    
    protected void incrCounters() {
      inputRecordCounter.increment(1);
      inputByteCounter.increment(afterPos - beforePos);
      fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
    }
     
    protected synchronized boolean moveToNext(K key, V value)
      throws IOException {
      boolean ret = false;
      try {
        reporter.setProgress(getProgress());
        beforePos = getPos();
        bytesInPrev = getInputBytes(fsStats);
        ret = rawIn.next(key, value);
        afterPos = getPos();
        bytesInCurr = getInputBytes(fsStats);
      } catch (IOException ioe) {
        if (split instanceof FileSplit) {
          LOG.error("IO error in map input file " + conf.get("map.input.file"));
          throw new IOException("IO error in map input file "
              + conf.get("map.input.file"), ioe);
        }
        throw ioe;
      }
      return ret;
    }

    public long getPos() throws IOException { return rawIn.getPos(); }
    
    public void close() throws IOException {
      bytesInPrev = getInputBytes(fsStats);
      rawIn.close(); 
      bytesInCurr = getInputBytes(fsStats);
      fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
    }
    
    public float getProgress() throws IOException {
      return rawIn.getProgress();
    }
    TaskReporter getTaskReporter() {
      return reporter;
    }
    
    private long getInputBytes(Statistics stats) {
      return stats == null ? 0 : stats.getBytesRead();
    }
  }

  /** Yanfeng
   * This class wraps the user's local record reader to update the counters and
   * progress as records are read.
   * 
   * give up, not as well as using IFile
   * @param <K>
   * @param <V>
   */
  public class IterationTrackedRecordReader<K, V> 
      implements RecordReader<K,V> {
    private RecordReader<K,V> rawIn;
    private Counters.Counter inputByteCounter;
    private Counters.Counter inputRecordCounter;
    private Counters.Counter fileInputByteCounter;
    private TaskReporter reporter;
    private InputSplit split;
    private long beforePos = -1;
    private long afterPos = -1;
    private long bytesInPrev = -1;
    private long bytesInCurr = -1;
    private final Statistics fsStats;

    IterationTrackedRecordReader(InputSplit split, JobConf job, TaskReporter reporter, InputFormat<K, V> inputformater)
        throws IOException {
    	
        inputRecordCounter = reporter.getCounter(MAP_INPUT_RECORDS);
        inputByteCounter = reporter.getCounter(MAP_INPUT_BYTES);
        fileInputByteCounter = reporter
            .getCounter(FileInputFormat.Counter.BYTES_READ);

        Statistics matchedStats = null;
        if (split instanceof FileSplit) {
          matchedStats = getFsStatistics(((FileSplit) split).getPath(), job);
        } 
        fsStats = matchedStats;
        
        bytesInPrev = getInputBytes(fsStats);

        rawIn = inputformater.getRecordReader(split, job, reporter);
        bytesInCurr = getInputBytes(fsStats);
        fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
        
        this.reporter = reporter;
        this.split = split;
        conf = job;
    }

    public K createKey() {
        return rawIn.createKey();
      }
        
      public V createValue() {
        return rawIn.createValue();
      }
       
      public synchronized boolean next(K key, V value)
      throws IOException {
        boolean ret = moveToNext(key, value);      
        if (ret) {
          incrCounters();
        }
        return ret;
      }
      
      protected void incrCounters() {
        inputRecordCounter.increment(1);
        inputByteCounter.increment(afterPos - beforePos);
        fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
      }
       
      protected synchronized boolean moveToNext(K key, V value)
        throws IOException {
        boolean ret = false;
        try {
          reporter.setProgress(getProgress());
          beforePos = getPos();
          bytesInPrev = getInputBytes(fsStats);
          ret = rawIn.next(key, value);
          afterPos = getPos();
          bytesInCurr = getInputBytes(fsStats);
        } catch (IOException ioe) {
            LOG.error("IO error in map input file " + conf.get("map.input.file"));
            throw new IOException("IO error in map input file "
                + conf.get("map.input.file"), ioe);
        }
        return ret;
      }

      public long getPos() throws IOException { return rawIn.getPos(); }
      
      public void close() throws IOException {
        bytesInPrev = getInputBytes(fsStats);
        rawIn.close(); 
        bytesInCurr = getInputBytes(fsStats);
        fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
      }
      
      public float getProgress() throws IOException {
        return rawIn.getProgress();
      }
      TaskReporter getTaskReporter() {
        return reporter;
      }
      
      private long getInputBytes(Statistics stats) {
        return stats == null ? 0 : stats.getBytesRead();
      }
  }
  
  /**
   * This class skips the records based on the failed ranges from previous 
   * attempts.
   */
  class SkippingRecordReader<K, V> extends TrackedRecordReader<K,V> {
    private SkipRangeIterator skipIt;
    private SequenceFile.Writer skipWriter;
    private boolean toWriteSkipRecs;
    private TaskUmbilicalProtocol umbilical;
    private Counters.Counter skipRecCounter;
    private long recIndex = -1;
    
    SkippingRecordReader(InputSplit split, TaskUmbilicalProtocol umbilical,
                         TaskReporter reporter) throws IOException {
      super(split, conf, reporter);
      this.umbilical = umbilical;
      this.skipRecCounter = reporter.getCounter(Counter.MAP_SKIPPED_RECORDS);
      this.toWriteSkipRecs = toWriteSkipRecs() &&  
          SkipBadRecords.getSkipOutputPath(conf)!=null;
      skipIt = getSkipRanges().skipRangeIterator();
    }
    
    public synchronized boolean next(K key, V value)
    throws IOException {
      if(!skipIt.hasNext()) {
        LOG.warn("Further records got skipped.");
        return false;
      }
      boolean ret = moveToNext(key, value);
      long nextRecIndex = skipIt.next();
      long skip = 0;
      while(recIndex<nextRecIndex && ret) {
        if(toWriteSkipRecs) {
          writeSkippedRec(key, value);
        }
      	ret = moveToNext(key, value);
        skip++;
      }
      //close the skip writer once all the ranges are skipped
      if(skip>0 && skipIt.skippedAllRanges() && skipWriter!=null) {
        skipWriter.close();
      }
      skipRecCounter.increment(skip);
      reportNextRecordRange(umbilical, recIndex);
      if (ret) {
        incrCounters();
      }
      return ret;
    }
    
    protected synchronized boolean moveToNext(K key, V value)
    throws IOException {
	    recIndex++;
      return super.moveToNext(key, value);
    }
    
    @SuppressWarnings("unchecked")
    private void writeSkippedRec(K key, V value) throws IOException{
      if(skipWriter==null) {
        Path skipDir = SkipBadRecords.getSkipOutputPath(conf);
        Path skipFile = new Path(skipDir, getTaskID().toString());
        skipWriter = 
          SequenceFile.createWriter(
              skipFile.getFileSystem(conf), conf, skipFile,
              (Class<K>) createKey().getClass(),
              (Class<V>) createValue().getClass(), 
              CompressionType.BLOCK, getTaskReporter());
      }
      skipWriter.append(key, value);
    }
  }

  @Override
  public void run(final JobConf job, final TaskUmbilicalProtocol umbilical) 
    throws IOException, ClassNotFoundException, InterruptedException {
    this.umbilical = umbilical;

    // start thread that will handle communication with parent
    TaskReporter reporter = new TaskReporter(getProgress(), umbilical,
        jvmContext);
    reporter.startCommunicationThread();
    boolean useNewApi = job.getUseNewMapper();
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

    if (useNewApi) {
      runNewMapper(job, splitMetaInfo, umbilical, reporter);
    } else {
    	if(job.isIterative()){
    		//normal iterative job, separate state data and static data
    		runIterativeMapper(job, splitMetaInfo, umbilical, reporter);
    	}else if(job.isPreserve()){
    		runPreserveMapper(job, splitMetaInfo, umbilical, reporter);
    	}else if(job.isIncrementalStart()){
    		runIncrementalMapper(job, splitMetaInfo, umbilical, reporter);
    	}else if(job.isIncrementalIterative()){
    		runIncrementalIterativeMapper(job, splitMetaInfo, umbilical, reporter);
    	}else{
    		runOldMapper(job, splitMetaInfo, umbilical, reporter);
    	}
    }
    done(umbilical, reporter);
  }
  @SuppressWarnings("unchecked")
  private <T> T getSplitDetails(Path file, long offset)
   throws IOException {
    FileSystem fs = file.getFileSystem(conf);
    FSDataInputStream inFile = fs.open(file);
    inFile.seek(offset);
    String className = Text.readString(inFile);
    Class<T> cls;
    try {
      cls = (Class<T>) conf.getClassByName(className);
    } catch (ClassNotFoundException ce) {
      IOException wrap = new IOException("Split class " + className +
                                          " not found");
      wrap.initCause(ce);
      throw wrap;
    }
    SerializationFactory factory = new SerializationFactory(conf);
    Deserializer<T> deserializer =
      (Deserializer<T>) factory.getDeserializer(cls);
    deserializer.open(inFile);
    T split = deserializer.deserialize(null);
    long pos = inFile.getPos();
    getCounters().findCounter(
         Task.Counter.SPLIT_RAW_BYTES).increment(pos - offset);
    inFile.close();
    return split;
  }
  
  @SuppressWarnings("unchecked")
  private <INKEY,INVALUE,OUTKEY,OUTVALUE>
  void runOldMapper(final JobConf job,
                    final TaskSplitIndex splitIndex,
                    final TaskUmbilicalProtocol umbilical,
                    TaskReporter reporter
                    ) throws IOException, InterruptedException,
                             ClassNotFoundException {
    InputSplit inputSplit = getSplitDetails(new Path(splitIndex.getSplitLocation()),
           splitIndex.getStartOffset());

    updateJobWithSplit(job, inputSplit);
    reporter.setInputSplit(inputSplit);

    RecordReader<INKEY,INVALUE> in = isSkipping() ? 
        new SkippingRecordReader<INKEY,INVALUE>(inputSplit, umbilical, reporter) :
        new TrackedRecordReader<INKEY,INVALUE>(inputSplit, job, reporter);
    job.setBoolean("mapred.skip.on", isSkipping());


    int numReduceTasks = conf.getNumReduceTasks();
    LOG.info("numReduceTasks: " + numReduceTasks);
    MapOutputCollector collector = null;
    if (numReduceTasks > 0) {
      collector = new MapOutputBuffer(umbilical, job, reporter);
    } else { 
      collector = new DirectMapOutputCollector(umbilical, job, reporter);
    }
    MapRunnable<INKEY,INVALUE,OUTKEY,OUTVALUE> runner =
      ReflectionUtils.newInstance(job.getMapRunnerClass(), job);

    try {
      runner.run(in, new OldOutputCollector(collector, conf), reporter);
      collector.flush();
    } finally {
      //close
      in.close();                               // close input
      collector.close();
    }
  }

  @SuppressWarnings("unchecked")
  private <SK extends WritableComparable, SV, DK extends WritableComparable, DV, K2, V2>
  void runIterativeMapper(final JobConf job,
                    final TaskSplitIndex splitIndex,
                    final TaskUmbilicalProtocol umbilical,
                    TaskReporter reporter
                    ) throws IOException, InterruptedException,
                             ClassNotFoundException {
	long taskstart = new Date().getTime();
	
	int iterationNum = job.getIterationNum();
	  
	FileSystem hdfs = FileSystem.get(job);
	FileSystem localfs = FileSystem.getLocal(job);
	
	if(job.getStaticDataPath() == null) throw new IOException("we need input data which is usually the static data!!!");
	
	RecordReader<SK, SV> staticReader = null;
	RecordReader<DK, DV> dynamicReader = null;

	//parsing the static data one by one
	Path remoteStaticPath = new Path(job.getStaticDataPath() + "/" + getOutputName(getTaskID().getTaskID().getId()));
	Path localStaticPath = new Path("/tmp/iteroop/" + job.getIterativeAlgorithmID() + "/substatic." + this.getTaskID().getTaskID().getId());

	if(hdfs.exists(remoteStaticPath)){
		//if it doesn't exist the local static file, it means it is the first iteration, so copy it from hdfs
		if(!localfs.exists(localStaticPath)){
			hdfs.copyToLocalFile(remoteStaticPath, localStaticPath);
			LOG.info("copy remote static file " + remoteStaticPath + " to local disk" + localStaticPath + "!!!!!!!!!");
		}
		
		//load static data
		long staticfilelen = localfs.getFileStatus(localStaticPath).getLen();
		
		InputSplit inputSplit = new FileSplit(localStaticPath, 0, staticfilelen, null, true);
		staticReader = new IterationTrackedRecordReader<SK, SV>(inputSplit, job, reporter, job.getStaticInputFormat());
	}else{
		throw new IOException("acturally, there is no static data on the path you" +
				" have set! Please check the path and check the map task number " + remoteStaticPath);
	}
	
    IterativeMapper<SK, SV, DK, DV, K2, V2> mapper = (IterativeMapper<SK, SV, DK, DV, K2, V2>) ReflectionUtils.newInstance(job.getIterativeMapperClass(), job);
    Projector<SK, DK, DV> projector = ReflectionUtils.newInstance(job.getProjectorClass(), job); 
    
	//if have state data or the following job (not the first job), prepare these state data files
	if(job.getDynamicDataPath() != null || job.getGlobalUniqValuePath() != null){
		if(projector.getProjectType() == Projector.Type.ONE2ALL){
			//global dynamic data, get data from hdfs
			Path globaldatapath = new Path(job.getGlobalUniqValuePath() + "/iteration-" + (iterationNum-1));
			
			long globaldatalen = hdfs.getFileStatus(globaldatapath).getLen();
			
			InputSplit inputSplit = new FileSplit(globaldatapath, 0, globaldatalen, job);
			dynamicReader = new IterationTrackedRecordReader<DK, DV>(inputSplit, job, reporter, job.getDynamicInputFormat());
		}else{
			Path localStatePath = new Path("/tmp/iteroop/" + job.getIterativeAlgorithmID() + "/substate-" + (iterationNum-1) + "." + this.getTaskID().getTaskID().getId());
			
			//this may happen when speculative execution enabled
			if(!localfs.exists(localStatePath)){
				//no state data on hdfs
				if(job.getCheckPointInterval() != 1)
					throw new IOException("no local dynamic data " + localStatePath);
				
				//have state data on hdfs
				Path remoteStatePath = new Path(job.getDynamicDataPath() + "/" + getOutputName(getTaskID().getTaskID().getId()));
				hdfs.copyToLocalFile(remoteStatePath, localStatePath);
				
				LOG.info("copy remote state file " + remoteStatePath + " to local disk" + localStatePath + "!!!!!!!!!");
			}
			
			//load state data
			long statefilelen = localfs.getFileStatus(localStatePath).getLen();
			
			InputSplit inputSplit = new FileSplit(localStatePath, 0, statefilelen, null, true);
			dynamicReader = new IterationTrackedRecordReader<DK, DV>(inputSplit, job, reporter, job.getDynamicInputFormat());
		}
	}


    
    SK statickeyObject = null;
    SV staticObject = null;
    DK dynamickeyObject = null;
    DV dynamicObject = null;

    //outputcollector
    int numReduceTasks = conf.getNumReduceTasks();
    LOG.info("numReduceTasks: " + numReduceTasks);
    MapOutputCollector<K2, V2> collector = null;
    if (numReduceTasks > 0) {
      collector = new MapOutputBuffer<K2, V2>(umbilical, job, reporter);
    } else { 
      collector = new DirectMapOutputCollector<K2, V2>(umbilical, job, reporter);
    }
    
    try{
		if(iterationNum == 1 && job.getDynamicDataPath() == null){
			//init dyanmic value based on user setting when running the first iterative job
			
			OldOutputCollector<K2, V2> output = new OldOutputCollector<K2, V2>(collector, conf);

		      // allocate key & value instances that are re-used for all entries
			statickeyObject = staticReader.createKey();
			staticObject = staticReader.createValue();

	        while (staticReader.next(statickeyObject, staticObject)) {
				dynamicObject = projector.initDynamicV(projector.project(statickeyObject));
				mapper.map(statickeyObject, staticObject, dynamicObject, output, reporter);
	        }
			
			collector.flush();
		}else{
			OldOutputCollector<K2, V2> output = new OldOutputCollector<K2, V2>(collector, conf);

		      // allocate key & value instances that are re-used for all entries
			statickeyObject = staticReader.createKey();
			staticObject = staticReader.createValue();
			dynamickeyObject = dynamicReader.createKey();
			dynamicObject = dynamicReader.createValue();

			//different parsing method for different join type
			Projector.Type joinType = projector.getProjectType();
			if(joinType == Projector.Type.ONE2ONE){
				//parse two files, and match the static key and dynamic key, and perform map function
		        while (staticReader.next(statickeyObject, staticObject)) {
		        	dynamicReader.next(dynamickeyObject, dynamicObject);
		        	
		        	if(!projector.project(statickeyObject).equals(dynamickeyObject)){
		        		throw new IOException("one2one key doesn't match!! " + statickeyObject + "\t" + dynamickeyObject);
		        	}
		        	
					mapper.map(statickeyObject, staticObject, dynamicObject, output, reporter);
		        }
			}else if(joinType == Projector.Type.ONE2ALL){
				//parse two files, and match the static key and dynamic key, and perform map function
				dynamicReader.next(dynamickeyObject, dynamicObject);
		        while (staticReader.next(statickeyObject, staticObject)) {
					mapper.map(statickeyObject, staticObject, dynamicObject, output, reporter);
		        }
			}else if(joinType == Projector.Type.ONE2MUL){
				//parse two files, and match the static key and dynamic key, and perform map function
				dynamicReader.next(dynamickeyObject, dynamicObject);		        	//to avoid apply project function multiple times
	        	DK projectedDynamicKey = projector.project(statickeyObject);
	        	
	        	//LOG.info(statickeyObject + "\t" + statekeyObject);
				while(!projectedDynamicKey.equals(dynamickeyObject)){
		        	
		        	//LOG.info(statickeyObject + "\t" + statekeyObject);
					while(!projector.project(statickeyObject).equals(dynamickeyObject)){
						LOG.info("dynamic key move next " + statickeyObject + "\t" + dynamickeyObject + " move next");
						dynamicReader.next(dynamickeyObject, dynamicObject);
					}
					mapper.map(statickeyObject, staticObject, dynamicObject, output, reporter);
		        }
			}
			
			collector.flush();
		}
    }finally{
    	if(staticReader != null) staticReader.close();
    	if(dynamicReader != null) dynamicReader.close();
    	collector.close();
    }

    long taskend = new Date().getTime();
    
    //report iterative task information
    IterativeTaskCompletionEvent event = new IterativeTaskCompletionEvent(job.getIterativeAlgorithmID(), getJobID(), job.getIterationNum(), 
    		this.getTaskID(), this.getTaskID().getTaskID().getId(), this.isMapTask());
    event.setProcessedRecords(reporter.getCounter(MAP_INPUT_RECORDS).getCounter());
    event.setRunTime(taskend - taskstart);
    
	try {
		umbilical.iterativeTaskComplete(event);
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }
  
  
  @SuppressWarnings("unchecked")
  private <SK extends WritableComparable, SV, DK extends WritableComparable, DV, K2, V2>
  void runPreserveMapper(final JobConf job,
                    final TaskSplitIndex splitIndex,
                    final TaskUmbilicalProtocol umbilical,
                    TaskReporter reporter
                    ) throws IOException, InterruptedException,
                             ClassNotFoundException {
		long taskstart = new Date().getTime();
		
		int iterationNum = job.getIterationNum();
		  
		FileSystem hdfs = FileSystem.get(job);
		FileSystem localfs = FileSystem.getLocal(job);

		if(job.getStaticDataPath() == null) throw new IOException("we need input data which is usually the static data!!!");
		
		RecordReader<SK, SV> staticReader = null;
		RecordReader<DK, DV> dynamicReader = null;

		//parsing the static data one by one
		Path remoteStaticPath = new Path(job.getStaticDataPath() + "/" + getOutputName(getTaskID().getTaskID().getId()));
		Path localStaticPath = new Path("/tmp/iteroop/" + job.getIterativeAlgorithmID() + "/substatic." + this.getTaskID().getTaskID().getId());

		if(hdfs.exists(remoteStaticPath)){
			//if it doesn't exist the local static file, it means it is the first iteration, so copy it from hdfs
			if(!localfs.exists(localStaticPath)){
				hdfs.copyToLocalFile(remoteStaticPath, localStaticPath);
				LOG.info("copy remote static file " + remoteStaticPath + " to local disk" + localStaticPath + "!!!!!!!!!");
			}
			
			//load static data
			long staticfilelen = localfs.getFileStatus(localStaticPath).getLen();
			
			InputSplit inputSplit = new FileSplit(localStaticPath, 0, staticfilelen, null, true);
			staticReader = new IterationTrackedRecordReader<SK, SV>(inputSplit, job, reporter, job.getStaticInputFormat());
		}else{
			throw new IOException("acturally, there is no static data on the path you" +
					" have set! Please check the path and check the map task number " + remoteStaticPath);
		}
		
	    IterativeMapper<SK, SV, DK, DV, K2, V2> mapper = (IterativeMapper<SK, SV, DK, DV, K2, V2>) ReflectionUtils.newInstance(job.getIterativeMapperClass(), job);
	    Projector<SK, DK, DV> projector = ReflectionUtils.newInstance(job.getProjectorClass(), job); 
	    
		//if have state data or the following job (not the first job), prepare these state data files
	
		if(projector.getProjectType() == Projector.Type.ONE2ALL){
			//global dynamic data, get data from hdfs
			Path globaldatapath = new Path(job.getGlobalUniqValuePath() + "/iteration-" + (job.getIterationNum()-1));
			
			long globaldatalen = hdfs.getFileStatus(globaldatapath).getLen();
			
			InputSplit inputSplit = new FileSplit(globaldatapath, 0, globaldatalen, job);
			dynamicReader = new IterationTrackedRecordReader<DK, DV>(inputSplit, job, reporter, job.getDynamicInputFormat());
		}else{
			Path localStatePath = new Path("/tmp/iteroop/" + job.getIterativeAlgorithmID() + "/substate-" + (job.getIterationNum()-1) + "." + this.getTaskID().getTaskID().getId());
			
			//this may happen when speculative execution enabled
			if(!localfs.exists(localStatePath)){
				//no state data on hdfs
				if(job.getCheckPointInterval() != 1)
					throw new IOException("no local dynamic data " + localStatePath);
				
				//have state data on hdfs
				String outputDir = job.get("mapred.output.dir");
				String lastOutputDir = outputDir.substring(0, outputDir.indexOf("-")) + "-" + (job.getIterationNum() - 1);
				Path remoteStatePath = new Path(lastOutputDir + "/" + getOutputName(getTaskID().getTaskID().getId()));
				hdfs.copyToLocalFile(remoteStatePath, localStatePath);
				LOG.info("copy remote state file " + remoteStatePath + " to local disk" + localStatePath + "!!!!!!!!!");
			}
			
			//load state data
			long statefilelen = localfs.getFileStatus(localStatePath).getLen();
			
			InputSplit inputSplit = new FileSplit(localStatePath, 0, statefilelen, null, true);
			dynamicReader = new IterationTrackedRecordReader<DK, DV>(inputSplit, job, reporter, job.getDynamicInputFormat());
		}
		
	    SK statickeyObject = null;
	    SV staticObject = null;
	    DK dynamickeyObject = null;
	    DV dynamicObject = null;

	    //outputcollector
	    int numReduceTasks = conf.getNumReduceTasks();
	    LOG.info("numReduceTasks: " + numReduceTasks);
	    MapOutputBufferwSource<SK, K2, V2> collector = new MapOutputBufferwSource<SK, K2, V2>(umbilical, job, reporter);
	    
	    try{
	    	OutputCollectorwSource<SK, K2, V2> output = new OutputCollectorwSource<SK, K2, V2>(collector, conf, mapper.removeLable());

		    // allocate key & value instances that are re-used for all entries
			statickeyObject = staticReader.createKey();
			staticObject = staticReader.createValue();
			dynamickeyObject = dynamicReader.createKey();
			dynamicObject = dynamicReader.createValue();

			//different parsing method for different join type
			Projector.Type joinType = projector.getProjectType();
			if(joinType == Projector.Type.ONE2ONE){
				LOG.info("starting parsing");
				//parse two files, and match the static key and dynamic key, and perform map function
		        while (staticReader.next(statickeyObject, staticObject)) {
		        	dynamicReader.next(dynamickeyObject, dynamicObject);
		        	
		        	if(!projector.project(statickeyObject).equals(dynamickeyObject)){
		        		throw new IOException("one2one key doesn't match!! " + statickeyObject + "\t" + dynamickeyObject);
		        	}
		        	
		        	//must first set the source key, then the preserver can record it and write to state file
		        	output.setSourceKey(statickeyObject);
					mapper.map(statickeyObject, staticObject, dynamicObject, output, reporter);
		        }
			}else if(joinType == Projector.Type.ONE2ALL){
				//parse two files, and match the static key and dynamic key, and perform map function
				dynamicReader.next(dynamickeyObject, dynamicObject);
		        while (staticReader.next(statickeyObject, staticObject)) {
		        	//must first set the source key, then the preserver can record it and write to state file
		        	output.setSourceKey(statickeyObject);
					mapper.map(statickeyObject, staticObject, dynamicObject, output, reporter);
		        }
			}else if(joinType == Projector.Type.ONE2MUL){
				//parse two files, and match the static key and dynamic key, and perform map function
				dynamicReader.next(dynamickeyObject, dynamicObject);
		        while (staticReader.next(statickeyObject, staticObject)) {
		        	
		        	//to avoid apply project function multiple times
		        	DK projectedDynamicKey = projector.project(statickeyObject);
		        	
		        	//LOG.info(statickeyObject + "\t" + statekeyObject);
					while(!projectedDynamicKey.equals(dynamickeyObject)){
						LOG.info("dynamic key move next " + statickeyObject + "\t" + dynamickeyObject + " move next");
						dynamicReader.next(dynamickeyObject, dynamicObject);
					}
					
					//must first set the source key, then the preserver can record it and write to state file
		        	output.setSourceKey(statickeyObject);
					mapper.map(statickeyObject, staticObject, dynamicObject, output, reporter);
		        }
			}
			
			collector.flush();
	    }finally{
	    	if(staticReader != null) staticReader.close();
	    	if(dynamicReader != null) dynamicReader.close();
	    	collector.close();
	    }

	    long taskend = new Date().getTime();
	    
	    //report iterative task information
	    IterativeTaskCompletionEvent event = new IterativeTaskCompletionEvent(job.getIterativeAlgorithmID(), getJobID(), job.getIterationNum(), 
	    		this.getTaskID(), this.getTaskID().getTaskID().getId(), this.isMapTask());
	    event.setProcessedRecords(reporter.getCounter(MAP_INPUT_RECORDS).getCounter());
	    event.setRunTime(taskend - taskstart);
	    
		try {
			umbilical.iterativeTaskComplete(event);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
  }
 /* 
  @SuppressWarnings("unchecked")
  private <SK extends WritableComparable, SV, DK extends WritableComparable, DV, K2, V2 extends Writable>
  void runIncrementalMapper(final JobConf job,
                    final TaskSplitIndex splitIndex,
                    final TaskUmbilicalProtocol umbilical,
                    TaskReporter reporter
                    ) throws IOException, InterruptedException,
                             ClassNotFoundException {
	long taskstart = new Date().getTime();

	hdfs = FileSystem.get(job);
	localfs = FileSystem.getLocal(job);

	RecordReader<SK, SV> staticReader = getStaticReader(job, reporter);
	IFile.TrippleReader<SK, SV, Text> deltaStaticReader = getDeltaStaticReader(job);		//delta update static file reader
	//IFile.TrippleReader<SK, K2, V2> preserveReader = getPreserveReader(job);		//preserve data reader
	RecordReader<DK, DV> dynamicReader = getDynamicReader(job, reporter);					//converged result reader
	//RecordWriter<SK, SV> staticWriter = getIncrementalStaticWriter(job, reporter);
	
    IterativeMapper<SK, SV, DK, DV, K2, V2> mapper = (IterativeMapper<SK, SV, DK, DV, K2, V2>) ReflectionUtils.newInstance(job.getIterativeMapperClass(), job);
    Projector<SK, DK, DV> projector = ReflectionUtils.newInstance(job.getProjectorClass(), job); 

    SK statickeyObject = null;
    SV staticvalObject = null;
    
    DK dynamickeyObject = null;
    DV dynamicvalObject = null;
    
    SK srckeyObject = null;
    K2 destkeyObject = null;
    V2 outvalObject = null;
    
    //for reading object from IFIle
	Deserializer deltakeyDeserializer;	
	Deserializer deltavalDeserializer;		
	Deserializer changeDeserializer;
	SerializationFactory serializationFactory = new SerializationFactory(job);
	deltakeyDeserializer = serializationFactory.getDeserializer(job.getStaticKeyClass());
	deltavalDeserializer = serializationFactory.getDeserializer(job.getStaticValueClass());
	changeDeserializer = serializationFactory.getDeserializer(Text.class);
	DataInputBuffer skeybuffer = new DataInputBuffer();
	DataInputBuffer svaluebuffer = new DataInputBuffer();
	DataInputBuffer changebuffer = new DataInputBuffer();

    //outputcollector
    int numReduceTasks = conf.getNumReduceTasks();
    LOG.info("numReduceTasks: " + numReduceTasks);
    MapOutputBufferwSource<SK, K2, V2> collector = null;
    if (numReduceTasks > 0) {
      collector = new MapOutputBufferwSource<SK, K2, V2>(umbilical, job, reporter);
    } else { 
      throw new RuntimeException("not implemented right now!");
    }
    
	OutputCollectorwSource<SK, K2, V2> output = new OutputCollectorwSource<SK, K2, V2>(collector, conf, mapper.removeLable());
	
    try{
		if(job.getIterationNum() == 1){
			//if the first iteration, the input is the static changed delta file

			dynamickeyObject = dynamicReader.createKey();
			dynamicvalObject = dynamicReader.createValue();
			Text changeType = new Text();

			//different parsing method for different join type
			Projector.Type joinType = projector.getProjectType();
			if(joinType == Projector.Type.ONE2ONE){
				//parse old static file, and match the changed static key
				boolean more = dynamicReader.next(dynamickeyObject, dynamicvalObject);
				if(!more) throw new RuntimeException("no more data in the dynamic data file!!! " + job.getDynamicDataPath());
				
				while(deltaStaticReader.next(skeybuffer, svaluebuffer, changebuffer)){
					deltakeyDeserializer.open(skeybuffer);
					deltavalDeserializer.open(svaluebuffer);
					changeDeserializer.open(changebuffer);
					statickeyObject = (SK)deltakeyDeserializer.deserialize(statickeyObject);
					staticvalObject = (SV)deltavalDeserializer.deserialize(staticvalObject);
					changeType = (Text)changeDeserializer.deserialize(changeType);
					
					DK projectedDynamicKey = projector.project(statickeyObject);
					
					LOG.info("delta read: " + statickeyObject + "\t" + staticvalObject + "\t" + changeType);
					
					while(!projectedDynamicKey.equals(dynamickeyObject)){
						dynamicReader.next(dynamickeyObject, dynamicvalObject);
						//LOG.info("dynamic data read: " + dynamickeyObject + "\t" + dynamicvalObject);
					}
					
					output.setSourceKey(statickeyObject);
					if(changeType.toString().equals("+")){
						output.setAdd(true);
					}else{
						output.setAdd(false);
					}
					mapper.map(statickeyObject, staticvalObject, dynamicvalObject, output, reporter);
				}
			}else if(joinType == Projector.Type.ONE2ALL){

			}else if(joinType == Projector.Type.ONE2MUL){

			}
			
			collector.flush();
		}else{
			//the input the new generated/reduce output dynamic file
			
			dynamickeyObject = dynamicReader.createKey();
			dynamicvalObject = dynamicReader.createValue();

			//different parsing method for different join type
			Projector.Type joinType = projector.getProjectType();
			if(joinType == Projector.Type.ONE2ONE){
				//parse old static file, and match the changed static key
				boolean more = staticReader.next(statickeyObject, staticvalObject);
				if(!more) throw new RuntimeException("no more data in the static data file!!! " + job.getStaticDataPath());
				
				while(dynamicReader.next(dynamickeyObject, dynamicvalObject)){
					while(!projector.project(statickeyObject).equals(dynamickeyObject)){
						staticReader.next(statickeyObject, staticvalObject);
					}
					
					output.setSourceKey(statickeyObject);
					mapper.map(statickeyObject, staticvalObject, dynamicvalObject, output, reporter);
				}
			}else if(joinType == Projector.Type.ONE2ALL){

			}else if(joinType == Projector.Type.ONE2MUL){

			}
			collector.flush();
		}
    }finally{
    	staticReader.close();
    	deltaStaticReader.close();	
    	//preserveReader.close();
    	dynamicReader.close();
    	collector.close();
    }

    long taskend = new Date().getTime();
    
    //report iterative task information
    IterativeTaskCompletionEvent event = new IterativeTaskCompletionEvent(job.getIterativeAlgorithmID(), getJobID(), job.getIterationNum(), 
    		this.getTaskID().getTaskID().getId(), this.isMapTask());
    event.setProcessedRecords(reporter.getCounter(MAP_INPUT_RECORDS).getCounter());
    event.setRunTime(taskend - taskstart);
    
	try {
		umbilical.iterativeTaskComplete(event);
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }
  */
  
  @SuppressWarnings("unchecked")
  private <SK extends WritableComparable, SV, DK extends WritableComparable, DV, K2, V2 extends Writable>
  void runIncrementalMapper(final JobConf job,
                    final TaskSplitIndex splitIndex,
                    final TaskUmbilicalProtocol umbilical,
                    TaskReporter reporter
                    ) throws IOException, InterruptedException,
                             ClassNotFoundException {
	long taskstart = new Date().getTime();

	hdfs = FileSystem.get(job);
	localfs = FileSystem.getLocal(job);

	IFile.TrippleReader<SK, SV, Text> deltaStaticReader = getDeltaStaticReader(job);		//delta update static file reader
	RecordReader<DK, DV> dynamicReader = getDynamicReader(job, -1, reporter);					//converged result reader
	
    IterativeMapper<SK, SV, DK, DV, K2, V2> mapper = (IterativeMapper<SK, SV, DK, DV, K2, V2>) ReflectionUtils.newInstance(job.getIterativeMapperClass(), job);
    Projector<SK, DK, DV> projector = ReflectionUtils.newInstance(job.getProjectorClass(), job); 

    SK deltaStatickeyObject = null;
    SV deltaStaticvalObject = null;
    
    DK dynamickeyObject = null;
    DV dynamicvalObject = null;
    
    //for reading object from IFIle
	Deserializer deltakeyDeserializer;	
	Deserializer deltavalDeserializer;		
	Deserializer changeDeserializer;
	SerializationFactory serializationFactory = new SerializationFactory(job);
	deltakeyDeserializer = serializationFactory.getDeserializer(job.getStaticKeyClass());
	deltavalDeserializer = serializationFactory.getDeserializer(job.getStaticValueClass());
	changeDeserializer = serializationFactory.getDeserializer(Text.class);
	DataInputBuffer skeybuffer = new DataInputBuffer();
	DataInputBuffer svaluebuffer = new DataInputBuffer();
	DataInputBuffer changebuffer = new DataInputBuffer();

    //outputcollector
    int numReduceTasks = conf.getNumReduceTasks();
    LOG.info("numReduceTasks: " + numReduceTasks);

    MapOutputBufferwSource<SK, K2, V2> collector = null;
    if (numReduceTasks > 0) {
      collector = new MapOutputBufferwSource<SK, K2, V2>(umbilical, job, reporter);
    } else { 
      throw new RuntimeException("not implemented right now!");
    }
	OutputCollectorwSource<SK, K2, V2> output = new OutputCollectorwSource<SK, K2, V2>(collector, conf, mapper.removeLable());

	//converged dynamic data
	dynamickeyObject = dynamicReader.createKey();
	dynamicvalObject = dynamicReader.createValue();
	Text changeType = new Text();
	
	//different parsing method for different join type
	Projector.Type joinType = projector.getProjectType();

	try{
		if(joinType == Projector.Type.ONE2ONE){
			//parse old static file and converged dynamic file, and match the changed static key
			boolean dynamicmore = dynamicReader.next(dynamickeyObject, dynamicvalObject);
			if(!dynamicmore) throw new RuntimeException("no more data in the dynamic data file!!! " + job.getDynamicDataPath());

			while(deltaStaticReader.next(skeybuffer, svaluebuffer, changebuffer)){
				deltakeyDeserializer.open(skeybuffer);
				deltavalDeserializer.open(svaluebuffer);
				changeDeserializer.open(changebuffer);
				deltaStatickeyObject = (SK)deltakeyDeserializer.deserialize(deltaStatickeyObject);
				deltaStaticvalObject = (SV)deltavalDeserializer.deserialize(deltaStaticvalObject);
				changeType = (Text)changeDeserializer.deserialize(changeType);
				
				DK projectedDynamicKey = projector.project(deltaStatickeyObject);
				
				LOG.info("delta read: " + deltaStatickeyObject + "\t" + deltaStaticvalObject + "\t" + changeType);
				
				while(!projectedDynamicKey.equals(dynamickeyObject)){
					dynamicReader.next(dynamickeyObject, dynamicvalObject);
					//LOG.info("dynamic data read: " + dynamickeyObject + "\t" + dynamicvalObject);
				}
				
				output.setSourceKey(deltaStatickeyObject);
				if(changeType.toString().equals("+")){
					output.setAdd(true);
				}else{
					output.setAdd(false);
				}
				mapper.map(deltaStatickeyObject, deltaStaticvalObject, dynamicvalObject, output, reporter);
			}
		}else if(joinType == Projector.Type.ONE2ALL){
	
		}else if(joinType == Projector.Type.ONE2MUL){
	
		}

	}finally{
		collector.flush();
		collector.close();
		deltaStaticReader.close();
		dynamicReader.close();
	}
	
    long taskend = new Date().getTime();
    
    //report iterative task information
    IterativeTaskCompletionEvent event = new IterativeTaskCompletionEvent(job.getIterativeAlgorithmID(), getJobID(), 0, 
    		this.getTaskID(), this.getTaskID().getTaskID().getId(), this.isMapTask());
    event.setProcessedRecords(reporter.getCounter(MAP_INPUT_RECORDS).getCounter());
    event.setRunTime(taskend - taskstart);
    
	try {
		umbilical.iterativeTaskComplete(event);
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }
  
  @SuppressWarnings("unchecked")
  private <SK extends WritableComparable, SV, DK extends WritableComparable, DV, K2, V2 extends Writable>
  void runIncrementalIterativeMapper(final JobConf job,
                    final TaskSplitIndex splitIndex,
                    final TaskUmbilicalProtocol umbilical,
                    TaskReporter reporter
                    ) throws IOException, InterruptedException,
                             ClassNotFoundException {
	long taskstart = new Date().getTime();

	hdfs = FileSystem.get(job);
	localfs = FileSystem.getLocal(job);

	RecordReader<SK, SV> staticReader = getStaticReader(job, reporter);
	RecordReader<DK, DV> dynamicReader = getFilterDynamicReader(job, job.getIterationNum()-1, reporter);					//converged result reader
	
    IterativeMapper<SK, SV, DK, DV, K2, V2> mapper = (IterativeMapper<SK, SV, DK, DV, K2, V2>) ReflectionUtils.newInstance(job.getIterativeMapperClass(), job);
    Projector<SK, DK, DV> projector = ReflectionUtils.newInstance(job.getProjectorClass(), job); 

    SK oldStatickeyObject = null;
    SV oldStaticvalObject = null;
    
    DK dynamickeyObject = null;
    DV dynamicvalObject = null;

    //outputcollector
    int numReduceTasks = conf.getNumReduceTasks();
    LOG.info("numReduceTasks: " + numReduceTasks);

    MapOutputBufferwSource<SK, K2, V2> collector = null;
    if (numReduceTasks > 0) {
      collector = new MapOutputBufferwSource<SK, K2, V2>(umbilical, job, reporter);
    } else { 
      throw new RuntimeException("not implemented right now!");
    }
	OutputCollectorwSource<SK, K2, V2> output = new OutputCollectorwSource<SK, K2, V2>(collector, conf, mapper.removeLable());

	
    try{
		//the input is the new generated/reduce output dynamic file
		dynamickeyObject = dynamicReader.createKey();
		dynamicvalObject = dynamicReader.createValue();
		
		//updated static data
		oldStatickeyObject = staticReader.createKey();
		oldStaticvalObject = staticReader.createValue();

		//different parsing method for different join type
		Projector.Type joinType = projector.getProjectType();
		
		if(joinType == Projector.Type.ONE2ONE){
			//parse old static file, and match the changed static key
			boolean more = staticReader.next(oldStatickeyObject, oldStaticvalObject);
			if(!more) throw new RuntimeException("no more data in the static data file!!! " + job.getStaticDataPath());
			
			while(dynamicReader.next(dynamickeyObject, dynamicvalObject)){
				//LOG.info("read dynamic " + dynamickeyObject + "\t" + dynamicvalObject);
				while(!projector.project(oldStatickeyObject).equals(dynamickeyObject)){
					more = staticReader.next(oldStatickeyObject, oldStaticvalObject);
					if(!more) break;
						
					//LOG.info("read static " + oldStatickeyObject + "\t" + oldStaticvalObject);
				}
				
				if(more){
					output.setSourceKey(oldStatickeyObject);
					
					//update the old one, update received value at the reduce side
					output.setAdd(true);
					
					mapper.map(oldStatickeyObject, oldStaticvalObject, dynamicvalObject, output, reporter);
				}
			}
		}else if(joinType == Projector.Type.ONE2ALL){

		}else if(joinType == Projector.Type.ONE2MUL){

		}

    }finally{
		collector.flush();
		collector.close();
    	staticReader.close();
    	dynamicReader.close();
    }

    long taskend = new Date().getTime();
    
    //report iterative task information
    IterativeTaskCompletionEvent event = new IterativeTaskCompletionEvent(job.getIterativeAlgorithmID(), getJobID(), job.getIterationNum(), 
    		this.getTaskID(), this.getTaskID().getTaskID().getId(), this.isMapTask());
    event.setProcessedRecords(reporter.getCounter(MAP_INPUT_RECORDS).getCounter());
    event.setRunTime(taskend - taskstart);
    
    LOG.info("finish task in " + (taskend - taskstart) + " s");
    
	try {
		umbilical.iterativeTaskComplete(event);
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }
  
  	private <SK, SV> RecordReader<SK, SV> getStaticReader(JobConf job, TaskReporter reporter) throws IOException{
	  	RecordReader<SK, SV> staticReader = null;
	  	
		if(job.getStaticDataPath() == null) throw new IOException("we need the old static data " +
				"to perform incremental computation!!!");
		
		//download the remote old static data
		Path remoteStaticPath = new Path(job.getStaticDataPath() + "/" + getOutputName(getTaskID().getTaskID().getId()));
		Path localStaticPath = new Path("/tmp/iteroop/" + job.getIterativeAlgorithmID() + "/static." + this.getTaskID().getTaskID().getId());
		
		if(hdfs.exists(remoteStaticPath)){
			//if it doesn't exist the local static file, it means it is the first iteration, so copy it from hdfs
			if(!localfs.exists(localStaticPath)){
				hdfs.copyToLocalFile(remoteStaticPath, localStaticPath);
				LOG.info("copy remote static file " + remoteStaticPath + " to local disk" + localStaticPath + "!!!!!!!!!");
			}
			
			//load static data
			long staticfilelen = localfs.getFileStatus(localStaticPath).getLen();
			
			InputSplit inputSplit = new FileSplit(localStaticPath, 0, staticfilelen, null, true);
			staticReader = new IterationTrackedRecordReader<SK, SV>(inputSplit, job, reporter, job.getStaticInputFormat());
		}else{
			throw new IOException("acturally, there is no static data on the path you" +
				" have set! Please check the path and check the map task number " + remoteStaticPath);
		}
		
		return staticReader;
	}
	
  	private <SK, SV> RecordReader<SK, SV> getUpdatedStaticReader(JobConf job, TaskReporter reporter) throws IOException{
  		RecordReader<SK, SV> staticReader = null;
	  	
		if(job.getStaticDataPath() == null) throw new IOException("we need the static data " +
				"path to store the new static data!!!");
		
		//download the remote old static data
		Path remoteStaticPath = new Path(job.getStaticDataPath() + "/" + getOutputName(getTaskID().getTaskID().getId()));
		Path localStaticPath = new Path("/tmp/iteroop/" + job.getIterativeAlgorithmID() + "/newstatic." + this.getTaskID().getTaskID().getId());
		
		if(hdfs.exists(remoteStaticPath)){
			//if it doesn't exist the local static file, it means it is the first iteration, so copy it from hdfs
			if(!localfs.exists(localStaticPath)){
				hdfs.copyToLocalFile(remoteStaticPath, localStaticPath);
				LOG.info("copy remote static file " + remoteStaticPath + " to local disk" + localStaticPath + "!!!!!!!!!");
			}
			
			//load static data
			long staticfilelen = localfs.getFileStatus(localStaticPath).getLen();
			
			InputSplit inputSplit = new FileSplit(localStaticPath, 0, staticfilelen, null, true);
			staticReader = new IterationTrackedRecordReader<SK, SV>(inputSplit, job, reporter, job.getStaticInputFormat());
		}else{
			throw new IOException("acturally, there is no static data on the path you" +
				" have set! Please check the path and check the map task number " + remoteStaticPath);
		}
		
		return staticReader;
	}
  	
	private <SK, SV> IFile.TrippleReader<SK, SV, Text> getDeltaStaticReader(JobConf job) throws IOException{
			IFile.TrippleReader<SK, SV, Text> deltaStaticReader = null;		//delta update static file reader
			
			if(job.getDeltaUpdatePath().equals("")) throw new IOException("we need input dela update data " +
					"to perform incremental computation!!!");
			
			//download the remote delta update data
			Path remoteDeltaStaticPath = new Path(job.getDeltaUpdatePath() + "/part-" + getTaskID().getTaskID().getId());
			Path localDeltaStaticPath = new Path("/tmp/iteroop/" + job.getIterativeAlgorithmID() + "/deltastatic." + this.getTaskID().getTaskID().getId());
	
			if(hdfs.exists(remoteDeltaStaticPath)){
				//if it doesn't exist the local static file, it means it is the first iteration, so copy it from hdfs
				if(!localfs.exists(localDeltaStaticPath)){
					hdfs.copyToLocalFile(remoteDeltaStaticPath, localDeltaStaticPath);
					LOG.info("copy remote static file " + remoteDeltaStaticPath + " to local disk" + localDeltaStaticPath + "!!!!!!!!!");
				}
				
				deltaStaticReader = new IFile.TrippleReader<SK, SV, Text>(job, localfs, localDeltaStaticPath, null, null);
			}else{
				throw new IOException("acturally, there is no static data on the path you" +
						" have set! Please check the path and check the map task number " + remoteDeltaStaticPath);
			}
			
			return deltaStaticReader;
	}
	
	private <SK,K2,V2> IFile.TrippleReader<SK, K2, V2> getPreserveReader(JobConf job) throws IOException{
		  IFile.TrippleReader<SK, K2, V2> preserveReader = null;		//preserve data reader
		  
			if(job.getPreserveStatePath() == null) throw new IOException("we need preserved data " +
					"to perform incremental computation!!!");
			
			//download the remote preserved update data
			Path remotePreservedStatePath = new Path(job.getPreserveStatePath() + "/mapPreserve-" + getTaskID().getTaskID().getId());
			Path localPreservedStatePath = new Path("/tmp/iteroop/" + job.getIterativeAlgorithmID() + "/mapPreserve." + this.getTaskID().getTaskID().getId());
	
			if(hdfs.exists(remotePreservedStatePath)){
				//if it doesn't exist the local static file, it means it is the first iteration, so copy it from hdfs
				if(!localfs.exists(localPreservedStatePath)){
					hdfs.copyToLocalFile(remotePreservedStatePath, localPreservedStatePath);
					LOG.info("copy remote static file " + remotePreservedStatePath + " to local disk" + localPreservedStatePath + "!!!!!!!!!");
				}
				
				preserveReader = new IFile.TrippleReader<SK, K2, V2>(job, localfs, localPreservedStatePath, null, null);
			}else{
				throw new IOException("acturally, there is no static data on the path you" +
						" have set! Please check the path and check the map task number " + remotePreservedStatePath);
			}
			
			return preserveReader;
	}
	
	private <DK,DV> RecordReader<DK, DV> getDynamicReader(JobConf job, int iteration, TaskReporter reporter) throws IOException{
			RecordReader<DK, DV> dynamicReader = null;					//converged result reader
			
			if(job.getDynamicDataPath() == null) throw new IOException("we need the converged dynamic data " +
					"to perform incremental computation!!!");
			
			//download the remote converged value data
			Path remoteDynamicPath = new Path(job.getDynamicDataPath() + "/" + getOutputName(getTaskID().getTaskID().getId()));
			Path localDynamicPath = new Path("/tmp/iteroop/" + job.getIterativeAlgorithmID() + "/substate-" + iteration + "." + this.getTaskID().getTaskID().getId());
	
			if(hdfs.exists(remoteDynamicPath)){
				//this may happen when speculative execution enabled
				if(!localfs.exists(localDynamicPath)){
					//if(iteration == -1){
						//if it doesn't exist the local static file, it means it is the first iteration, so copy it from hdfs
						hdfs.copyToLocalFile(remoteDynamicPath, localDynamicPath);
						LOG.info("copy remote state file " + remoteDynamicPath + " to local disk" + localDynamicPath + "!!!!!!!!!");
				
				}/*	else{
						//may happen when speculative execution
						if(job.getCheckPointInterval() != 1)
							throw new IOException("no local dynamic data " + localDynamicPath);
						
						//have state data on hdfs, copy them from last iteration output
						String outputDir = job.get("mapred.output.dir");
						String lastOutputDir = outputDir.substring(0, outputDir.indexOf("-")) + "-" + (iteration - 1);
						Path remoteStatePath = new Path(lastOutputDir + "/" + getOutputName(getTaskID().getTaskID().getId()));
						hdfs.copyToLocalFile(remoteStatePath, localDynamicPath);
						
						LOG.info("copy last iteration's remote state file " + remoteStatePath + " to local disk" + localDynamicPath + "!!!!!!!!!");
					}
				}*/
				
				//load state data
				long statefilelen = localfs.getFileStatus(localDynamicPath).getLen();
				
				InputSplit inputSplit = new FileSplit(localDynamicPath, 0, statefilelen, null, true);
				dynamicReader = new IterationTrackedRecordReader<DK, DV>(inputSplit, job, reporter, job.getDynamicInputFormat());
			}else{
				throw new IOException("acturally, there is no static data on the path you" +
						" have set! Please check the path and check the map task number " + remoteDynamicPath);
			}
			
			LOG.info("processing dynamic data file " + localDynamicPath);
			
			return dynamicReader;
	}
	
	private <DK,DV> RecordReader<DK, DV> getFilterDynamicReader(JobConf job, int iteration, TaskReporter reporter) throws IOException{
		RecordReader<DK, DV> dynamicReader = null;					//converged result reader
		
		if(job.getDynamicDataPath() == null) throw new IOException("we need the converged dynamic data " +
				"to perform incremental computation!!!");
		
		//download the remote converged value data
		Path remoteDynamicPath = new Path(job.getDynamicDataPath() + "/" + getOutputName(getTaskID().getTaskID().getId()));
		Path localDynamicPath = new Path("/tmp/iteroop/" + job.getIterativeAlgorithmID() + "/filter-" + iteration + "." + this.getTaskID().getTaskID().getId());

		//if(hdfs.exists(remoteDynamicPath)){
			//this may happen when speculative execution enabled
			if(!localfs.exists(localDynamicPath)){
				//if(iteration == -1){
					//if it doesn't exist the local static file, it means it is the first iteration, so copy it from hdfs
					//hdfs.copyToLocalFile(remoteDynamicPath, localDynamicPath);
					//LOG.info("copy remote state file " + remoteDynamicPath + " to local disk" + localDynamicPath + "!!!!!!!!!");
				throw new RuntimeException("no file " + localDynamicPath + " found");
			}
			
			//load state data
			long statefilelen = localfs.getFileStatus(localDynamicPath).getLen();
			
			InputSplit inputSplit = new FileSplit(localDynamicPath, 0, statefilelen, null, true);
			dynamicReader = new IterationTrackedRecordReader<DK, DV>(inputSplit, job, reporter, job.getDynamicInputFormat());
		//}else{
			//throw new IOException("acturally, there is no static data on the path you" +
			//		" have set! Please check the path and check the map task number " + remoteDynamicPath);
		//}
		
		LOG.info("processing dynamic data file " + localDynamicPath);
		
		return dynamicReader;
}
  /*
	private <DK,DV> RecordReader<DK, DV> getIncrPrevReader(JobConf job, int iteration, TaskReporter reporter) throws IOException{
		RecordReader<DK, DV> dynamicReader = null;					//converged result reader
		
		//download the remote converged value data
		Path remoteDynamicPath = new Path(job.getIncrOutputPath() + "/iteration-" + iteration + "/" + getOutputName(getTaskID().getTaskID().getId()));
		Path localDynamicPath = new Path("/tmp/iteroop/" + job.getIterativeAlgorithmID() + "/substate-" + iteration+ "." + this.getTaskID().getTaskID().getId());

		if(!localfs.exists(localDynamicPath)){
			if(hdfs.exists(remoteDynamicPath)){
				
			}
		}else{
			
		}
		if(hdfs.exists(remoteDynamicPath)){
			//this may happen when speculative execution enabled
			if(!localfs.exists(localDynamicPath)){
				if(job.getIterationNum() == 1){
					//if it doesn't exist the local static file, it means it is the first iteration, so copy it from hdfs
					hdfs.copyToLocalFile(remoteDynamicPath, localDynamicPath);
					LOG.info("copy remote state file " + remoteDynamicPath + " to local disk" + localDynamicPath + "!!!!!!!!!");
				}else{
					//may happen when speculative execution
					if(job.getCheckPointInterval() != 1)
						throw new IOException("no local dynamic data " + localDynamicPath);
					
					//have state data on hdfs, copy them from last iteration output
					String outputDir = job.get("mapred.output.dir");
					String lastOutputDir = outputDir.substring(0, outputDir.indexOf("-")) + "-" + (job.getIterationNum() - 1);
					Path remoteStatePath = new Path(lastOutputDir + "/" + getOutputName(getTaskID().getTaskID().getId()));
					hdfs.copyToLocalFile(remoteStatePath, localDynamicPath);
					
					LOG.info("copy last iteration's remote state file " + remoteStatePath + " to local disk" + localDynamicPath + "!!!!!!!!!");
				}
			}
			
			//load state data
			long statefilelen = localfs.getFileStatus(localDynamicPath).getLen();
			
			InputSplit inputSplit = new FileSplit(localDynamicPath, 0, statefilelen, null, true);
			dynamicReader = new IterationTrackedRecordReader<DK, DV>(inputSplit, job, reporter, job.getDynamicInputFormat());
		}else{
			throw new IOException("acturally, there is no static data on the path you" +
					" have set! Please check the path and check the map task number " + remoteDynamicPath);
		}
		
		return dynamicReader;
	}
	*/
  /**
   * Update the job with details about the file split
   * @param job the job configuration to update
   * @param inputSplit the file split
   */
  private void updateJobWithSplit(final JobConf job, InputSplit inputSplit) {
    if (inputSplit instanceof FileSplit) {
      FileSplit fileSplit = (FileSplit) inputSplit;
      job.set("map.input.file", fileSplit.getPath().toString());
      job.setLong("map.input.start", fileSplit.getStart());
      job.setLong("map.input.length", fileSplit.getLength());
    }
  }

  static class NewTrackingRecordReader<K,V> 
    extends org.apache.hadoop.mapreduce.RecordReader<K,V> {
    private final org.apache.hadoop.mapreduce.RecordReader<K,V> real;
    private final org.apache.hadoop.mapreduce.Counter inputRecordCounter;
    private final org.apache.hadoop.mapreduce.Counter fileInputByteCounter;
    private final TaskReporter reporter;
    private org.apache.hadoop.mapreduce.InputSplit inputSplit;
    private final JobConf job;
    private final Statistics fsStats;
    
    NewTrackingRecordReader(org.apache.hadoop.mapreduce.InputSplit split,
        org.apache.hadoop.mapreduce.InputFormat inputFormat,
        TaskReporter reporter, JobConf job,
        org.apache.hadoop.mapreduce.TaskAttemptContext taskContext)
        throws IOException, InterruptedException {
      this.reporter = reporter;
      this.inputSplit = split;
      this.job = job;
      this.inputRecordCounter = reporter.getCounter(MAP_INPUT_RECORDS);
      this.fileInputByteCounter = reporter
          .getCounter(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.Counter.BYTES_READ);

      Statistics matchedStats = null;
      if (split instanceof org.apache.hadoop.mapreduce.lib.input.FileSplit) {
        matchedStats = getFsStatistics(((org.apache.hadoop.mapreduce.lib.input.FileSplit) split)
            .getPath(), job);
      } 
      fsStats = matchedStats;
	  
      long bytesInPrev = getInputBytes(fsStats);
      this.real = inputFormat.createRecordReader(split, taskContext);
      long bytesInCurr = getInputBytes(fsStats);
      fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
    }

    @Override
    public void close() throws IOException {
      long bytesInPrev = getInputBytes(fsStats);
      real.close();
      long bytesInCurr = getInputBytes(fsStats);
      fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
    }

    @Override
    public K getCurrentKey() throws IOException, InterruptedException {
      return real.getCurrentKey();
    }

    @Override
    public V getCurrentValue() throws IOException, InterruptedException {
      return real.getCurrentValue();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return real.getProgress();
    }

    @Override
    public void initialize(org.apache.hadoop.mapreduce.InputSplit split,
                           org.apache.hadoop.mapreduce.TaskAttemptContext context
                           ) throws IOException, InterruptedException {
      long bytesInPrev = getInputBytes(fsStats);
      real.initialize(split, context);
      long bytesInCurr = getInputBytes(fsStats);
      fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      boolean result = false;
      try {
        long bytesInPrev = getInputBytes(fsStats);
        result = real.nextKeyValue();
        long bytesInCurr = getInputBytes(fsStats);

        if (result) {
          inputRecordCounter.increment(1);
          fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
        }
        reporter.setProgress(getProgress());
      } catch (IOException ioe) {
        if (inputSplit instanceof FileSplit) {
          FileSplit fileSplit = (FileSplit) inputSplit;
          LOG.error("IO error in map input file "
              + fileSplit.getPath().toString());
          throw new IOException("IO error in map input file "
              + fileSplit.getPath().toString(), ioe);
        }
        throw ioe;
      }
      return result;
    }
    
    private long getInputBytes(Statistics stats) {
      return stats == null ? 0 : stats.getBytesRead();
    }
  }

  /**
   * Since the mapred and mapreduce Partitioners don't share a common interface
   * (JobConfigurable is deprecated and a subtype of mapred.Partitioner), the
   * partitioner lives in Old/NewOutputCollector. Note that, for map-only jobs,
   * the configured partitioner should not be called. It's common for
   * partitioners to compute a result mod numReduces, which causes a div0 error
   */
  private static class OldOutputCollector<K,V> implements OutputCollector<K,V> {
    private final Partitioner<K,V> partitioner;
    private final MapOutputCollector<K,V> collector;
    private final int numPartitions;

    @SuppressWarnings("unchecked")
    OldOutputCollector(MapOutputCollector<K,V> collector, JobConf conf) {
      numPartitions = conf.getNumReduceTasks();
      if (numPartitions > 0) {
        partitioner = (Partitioner<K,V>)
          ReflectionUtils.newInstance(conf.getPartitionerClass(), conf);
      } else {
        partitioner = new Partitioner<K,V>() {
          @Override
          public void configure(JobConf job) { }
          @Override
          public int getPartition(K key, V value, int numPartitions) {
            return -1;
          }
        };
      }
      this.collector = collector;
    }

    @Override
    public void collect(K key, V value) throws IOException {
      try {
        collector.collect(key, value,
                          partitioner.getPartition(key, value, numPartitions));
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new IOException("interrupt exception", ie);
      }
    }
  }

  
  private static class OutputCollectorwSource<SK,IK,IV> implements OutputCollector<IK,IV> {
	    private final Partitioner<IK,IV> partitioner;
	    private final MapOutputBufferwSource<SK, IK,IV> collector;
	    private SK skey;
	    private final int numPartitions;
	    private boolean markorunmark = true;
	    private IV negativeV;

	    @SuppressWarnings("unchecked")
	    OutputCollectorwSource(MapOutputBufferwSource<SK, IK,IV> collector, JobConf conf, IV negativeV) {
	      numPartitions = conf.getNumReduceTasks();
	      if (numPartitions > 0) {
	        partitioner = (Partitioner<IK,IV>)
	          ReflectionUtils.newInstance(conf.getPartitionerClass(), conf);
	      } else {
	        partitioner = new Partitioner<IK,IV>() {
	          @Override
	          public void configure(JobConf job) { }
	          @Override
	          public int getPartition(IK key, IV value, int numPartitions) {
	            return -1;
	          }
	        };
	      }
	      this.collector = collector;
	      this.negativeV = negativeV;
	    }

	    @Override
	    public void collect(IK key, IV value) throws IOException {
	      try {
	    	  if(markorunmark){
	    		  //a new value, to be used as updated value
		    	  collector.collect(skey, key, value,
                          partitioner.getPartition(key, value, numPartitions));
		    	  LOG.info("update " + skey + "\t" + key + "\t" + value);
	    	  }else{
	    		  //a outdated value, send null, to notify to erase this value
		    	  collector.collect(skey, key, negativeV,
                          partitioner.getPartition(key, value, numPartitions));
		    	  LOG.info("remove " + skey + "\t" + key);
	    	  }

	      } catch (InterruptedException ie) {
	        Thread.currentThread().interrupt();
	        throw new IOException("interrupt exception", ie);
	      }
	    }

	    public void setSourceKey(SK skey){
	    	this.skey = skey;
	    }
	    
	    public void setAdd(boolean markOrunmark){
	    	markorunmark = markOrunmark;
	    }
	    
	  }
  
  private class NewDirectOutputCollector<K,V>
  extends org.apache.hadoop.mapreduce.RecordWriter<K,V> {
    private final org.apache.hadoop.mapreduce.RecordWriter out;

    private final TaskReporter reporter;

    private final Counters.Counter mapOutputRecordCounter;
    private final Counters.Counter fileOutputByteCounter; 
    private final Statistics fsStats;
    
    @SuppressWarnings("unchecked")
    NewDirectOutputCollector(org.apache.hadoop.mapreduce.JobContext jobContext,
        JobConf job, TaskUmbilicalProtocol umbilical, TaskReporter reporter) 
    throws IOException, ClassNotFoundException, InterruptedException {
      this.reporter = reporter;
      Statistics matchedStats = null;
      if (outputFormat instanceof org.apache.hadoop.mapreduce.lib.output.FileOutputFormat) {
        matchedStats = getFsStatistics(org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
            .getOutputPath(jobContext), job);
      }
      fsStats = matchedStats;
      mapOutputRecordCounter = 
        reporter.getCounter(MAP_OUTPUT_RECORDS);
      fileOutputByteCounter = reporter
          .getCounter(org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.Counter.BYTES_WRITTEN);

      long bytesOutPrev = getOutputBytes(fsStats);
      out = outputFormat.getRecordWriter(taskContext);
      long bytesOutCurr = getOutputBytes(fsStats);
      fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(K key, V value) 
    throws IOException, InterruptedException {
      reporter.progress();
      long bytesOutPrev = getOutputBytes(fsStats);
      out.write(key, value);
      long bytesOutCurr = getOutputBytes(fsStats);
      fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
      mapOutputRecordCounter.increment(1);
    }

    @Override
    public void close(TaskAttemptContext context) 
    throws IOException,InterruptedException {
      reporter.progress();
      if (out != null) {
        long bytesOutPrev = getOutputBytes(fsStats);
        out.close(context);
        long bytesOutCurr = getOutputBytes(fsStats);
        fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
      }
    }

    private long getOutputBytes(Statistics stats) {
      return stats == null ? 0 : stats.getBytesWritten();
    }
  }
  
  private class NewOutputCollector<K,V>
    extends org.apache.hadoop.mapreduce.RecordWriter<K,V> {
    private final MapOutputCollector<K,V> collector;
    private final org.apache.hadoop.mapreduce.Partitioner<K,V> partitioner;
    private final int partitions;

    @SuppressWarnings("unchecked")
    NewOutputCollector(org.apache.hadoop.mapreduce.JobContext jobContext,
                       JobConf job,
                       TaskUmbilicalProtocol umbilical,
                       TaskReporter reporter
                       ) throws IOException, ClassNotFoundException {
      collector = new MapOutputBuffer<K,V>(umbilical, job, reporter);
      partitions = jobContext.getNumReduceTasks();
      if (partitions > 0) {
        partitioner = (org.apache.hadoop.mapreduce.Partitioner<K,V>)
          ReflectionUtils.newInstance(jobContext.getPartitionerClass(), job);
      } else {
        partitioner = new org.apache.hadoop.mapreduce.Partitioner<K,V>() {
          @Override
          public int getPartition(K key, V value, int numPartitions) {
            return -1;
          }
        };
      }
    }

    @Override
    public void write(K key, V value) throws IOException, InterruptedException {
      collector.collect(key, value,
                        partitioner.getPartition(key, value, partitions));
    }

    @Override
    public void close(TaskAttemptContext context
                      ) throws IOException,InterruptedException {
      try {
        collector.flush();
      } catch (ClassNotFoundException cnf) {
        throw new IOException("can't find class ", cnf);
      }
      collector.close();
    }
  }

  @SuppressWarnings("unchecked")
  private <INKEY,INVALUE,OUTKEY,OUTVALUE>
  void runNewMapper(final JobConf job,
                    final TaskSplitIndex splitIndex,
                    final TaskUmbilicalProtocol umbilical,
                    TaskReporter reporter
                    ) throws IOException, ClassNotFoundException,
                             InterruptedException {
    // make a task context so we can get the classes
    org.apache.hadoop.mapreduce.TaskAttemptContext taskContext =
      new org.apache.hadoop.mapreduce.TaskAttemptContext(job, getTaskID());
    // make a mapper
    org.apache.hadoop.mapreduce.Mapper<INKEY,INVALUE,OUTKEY,OUTVALUE> mapper =
      (org.apache.hadoop.mapreduce.Mapper<INKEY,INVALUE,OUTKEY,OUTVALUE>)
        ReflectionUtils.newInstance(taskContext.getMapperClass(), job);
    // make the input format
    org.apache.hadoop.mapreduce.InputFormat<INKEY,INVALUE> inputFormat =
      (org.apache.hadoop.mapreduce.InputFormat<INKEY,INVALUE>)
        ReflectionUtils.newInstance(taskContext.getInputFormatClass(), job);
    // rebuild the input split
    org.apache.hadoop.mapreduce.InputSplit split = null;
    split = getSplitDetails(new Path(splitIndex.getSplitLocation()),
        splitIndex.getStartOffset());

    org.apache.hadoop.mapreduce.RecordReader<INKEY,INVALUE> input =
      new NewTrackingRecordReader<INKEY,INVALUE>
          (split, inputFormat, reporter, job, taskContext);

    job.setBoolean("mapred.skip.on", isSkipping());
    org.apache.hadoop.mapreduce.RecordWriter output = null;
    org.apache.hadoop.mapreduce.Mapper<INKEY,INVALUE,OUTKEY,OUTVALUE>.Context 
         mapperContext = null;
    try {
      Constructor<org.apache.hadoop.mapreduce.Mapper.Context> contextConstructor =
        org.apache.hadoop.mapreduce.Mapper.Context.class.getConstructor
        (new Class[]{org.apache.hadoop.mapreduce.Mapper.class,
                     Configuration.class,
                     org.apache.hadoop.mapreduce.TaskAttemptID.class,
                     org.apache.hadoop.mapreduce.RecordReader.class,
                     org.apache.hadoop.mapreduce.RecordWriter.class,
                     org.apache.hadoop.mapreduce.OutputCommitter.class,
                     org.apache.hadoop.mapreduce.StatusReporter.class,
                     org.apache.hadoop.mapreduce.InputSplit.class});

      // get an output object
      if (job.getNumReduceTasks() == 0) {
         output =
           new NewDirectOutputCollector(taskContext, job, umbilical, reporter);
      } else {
        output = new NewOutputCollector(taskContext, job, umbilical, reporter);
      }

      mapperContext = contextConstructor.newInstance(mapper, job, getTaskID(),
                                                     input, output, committer,
                                                     reporter, split);

      input.initialize(split, mapperContext);
      mapper.run(mapperContext);
      input.close();
      output.close(mapperContext);
    } catch (NoSuchMethodException e) {
      throw new IOException("Can't find Context constructor", e);
    } catch (InstantiationException e) {
      throw new IOException("Can't create Context", e);
    } catch (InvocationTargetException e) {
      throw new IOException("Can't invoke Context constructor", e);
    } catch (IllegalAccessException e) {
      throw new IOException("Can't invoke Context constructor", e);
    }
  }

  interface MapOutputCollector<K, V> {

    public void collect(K key, V value, int partition
                        ) throws IOException, InterruptedException;
    public void close() throws IOException, InterruptedException;
    
    public void flush() throws IOException, InterruptedException, 
                               ClassNotFoundException;
        
  }

  class DirectMapOutputCollector<K, V>
    implements MapOutputCollector<K, V> {
 
    private RecordWriter<K, V> out = null;

    private TaskReporter reporter = null;

    private final Counters.Counter mapOutputRecordCounter;
    private final Counters.Counter fileOutputByteCounter;
    private final Statistics fsStats;

    @SuppressWarnings("unchecked")
    public DirectMapOutputCollector(TaskUmbilicalProtocol umbilical,
        JobConf job, TaskReporter reporter) throws IOException {
      this.reporter = reporter;
      String finalName = getOutputName(getPartition());
      FileSystem fs = FileSystem.get(job);

      
      OutputFormat<K, V> outputFormat = job.getOutputFormat();
      
      Statistics matchedStats = null;
      if (outputFormat instanceof FileOutputFormat) {
        matchedStats = getFsStatistics(FileOutputFormat.getOutputPath(job), job);
      } 
      fsStats = matchedStats;
      mapOutputRecordCounter = reporter.getCounter(MAP_OUTPUT_RECORDS);
      fileOutputByteCounter = reporter
          .getCounter(FileOutputFormat.Counter.BYTES_WRITTEN);
      
      long bytesOutPrev = getOutputBytes(fsStats);
      out = job.getOutputFormat().getRecordWriter(fs, job, finalName, reporter);
      long bytesOutCurr = getOutputBytes(fsStats);
      fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
    }

    public void close() throws IOException {
      if (this.out != null) {
        long bytesOutPrev = getOutputBytes(fsStats);
        out.close(this.reporter);
        long bytesOutCurr = getOutputBytes(fsStats);
        fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
      }

    }

    public void flush() throws IOException, InterruptedException, 
                               ClassNotFoundException {
    }

    public void collect(K key, V value, int partition) throws IOException {
      reporter.progress();
      long bytesOutPrev = getOutputBytes(fsStats);
      out.write(key, value);
      long bytesOutCurr = getOutputBytes(fsStats);
      fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
      mapOutputRecordCounter.increment(1);
    }
    
    private long getOutputBytes(Statistics stats) {
      return stats == null ? 0 : stats.getBytesWritten();
    }
  }

  class MapOutputBuffer<K extends Object, V extends Object> 
  implements MapOutputCollector<K, V>, IndexedSortable {
    private final int partitions;
    private final JobConf job;
    private final TaskReporter reporter;
    private final Class<K> keyClass;
    private final Class<V> valClass;
    private final RawComparator<K> comparator;
    private final SerializationFactory serializationFactory;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valSerializer;
    private final CombinerRunner<K,V> combinerRunner;
    private final CombineOutputCollector<K, V> combineCollector;
    
    // Compression for map-outputs
    private CompressionCodec codec = null;

    // k/v accounting
    private volatile int kvstart = 0;  // marks beginning of spill
    private volatile int kvend = 0;    // marks beginning of collectable
    private int kvindex = 0;           // marks end of collected
    private final int[] kvoffsets;     // indices into kvindices
    private final int[] kvindices;     // partition, k/v offsets into kvbuffer
    private volatile int bufstart = 0; // marks beginning of spill
    private volatile int bufend = 0;   // marks beginning of collectable
    private volatile int bufvoid = 0;  // marks the point where we should stop
                                       // reading at the end of the buffer
    private int bufindex = 0;          // marks end of collected
    private int bufmark = 0;           // marks end of record
    private byte[] kvbuffer;           // main output buffer
    private static final int PARTITION = 0; // partition offset in acct
    private static final int KEYSTART = 1;  // key offset in acct
    private static final int VALSTART = 2;  // val offset in acct
    private static final int ACCTSIZE = 3;  // total #fields in acct
    private static final int RECSIZE =
                       (ACCTSIZE + 1) * 4;  // acct bytes per record

    // spill accounting
    private volatile int numSpills = 0;
    private volatile Throwable sortSpillException = null;
    private final int softRecordLimit;
    private final int softBufferLimit;
    private final int minSpillsForCombine;
    private final IndexedSorter sorter;
    private final ReentrantLock spillLock = new ReentrantLock();
    private final Condition spillDone = spillLock.newCondition();
    private final Condition spillReady = spillLock.newCondition();
    private final BlockingBuffer bb = new BlockingBuffer();
    private volatile boolean spillThreadRunning = false;
    private final SpillThread spillThread = new SpillThread();

    private final FileSystem localFs;
    private final FileSystem rfs;
   
    private final Counters.Counter mapOutputByteCounter;
    private final Counters.Counter mapOutputRecordCounter;
    private final Counters.Counter combineOutputCounter;
    private final Counters.Counter fileOutputByteCounter;
    
    private ArrayList<SpillRecord> indexCacheList;
    private int totalIndexCacheMemory;
    private static final int INDEX_CACHE_MEMORY_LIMIT = 1024 * 1024;

    @SuppressWarnings("unchecked")
    public MapOutputBuffer(TaskUmbilicalProtocol umbilical, JobConf job,
                           TaskReporter reporter
                           ) throws IOException, ClassNotFoundException {
      this.job = job;
      this.reporter = reporter;
      localFs = FileSystem.getLocal(job);
      partitions = job.getNumReduceTasks();
       
      rfs = ((LocalFileSystem)localFs).getRaw();

      indexCacheList = new ArrayList<SpillRecord>();
      
      //sanity checks
      final float spillper = job.getFloat("io.sort.spill.percent",(float)0.8);
      final float recper = job.getFloat("io.sort.record.percent",(float)0.05);
      final int sortmb = job.getInt("io.sort.mb", 100);
      if (spillper > (float)1.0 || spillper < (float)0.0) {
        throw new IOException("Invalid \"io.sort.spill.percent\": " + spillper);
      }
      if (recper > (float)1.0 || recper < (float)0.01) {
        throw new IOException("Invalid \"io.sort.record.percent\": " + recper);
      }
      if ((sortmb & 0x7FF) != sortmb) {
        throw new IOException("Invalid \"io.sort.mb\": " + sortmb);
      }
      sorter = ReflectionUtils.newInstance(
            job.getClass("map.sort.class", QuickSort.class, IndexedSorter.class), job);
      LOG.info("io.sort.mb = " + sortmb);
      // buffers and accounting
      int maxMemUsage = sortmb << 20;
      int recordCapacity = (int)(maxMemUsage * recper);
      recordCapacity -= recordCapacity % RECSIZE;
      kvbuffer = new byte[maxMemUsage - recordCapacity];
      bufvoid = kvbuffer.length;
      recordCapacity /= RECSIZE;
      kvoffsets = new int[recordCapacity];
      kvindices = new int[recordCapacity * ACCTSIZE];
      softBufferLimit = (int)(kvbuffer.length * spillper);
      softRecordLimit = (int)(kvoffsets.length * spillper);
      LOG.info("data buffer = " + softBufferLimit + "/" + kvbuffer.length);
      LOG.info("record buffer = " + softRecordLimit + "/" + kvoffsets.length);
      // k/v serialization
      comparator = job.getOutputKeyComparator();
      keyClass = (Class<K>)job.getMapOutputKeyClass();
      valClass = (Class<V>)job.getMapOutputValueClass();
      serializationFactory = new SerializationFactory(job);
      keySerializer = serializationFactory.getSerializer(keyClass);
      keySerializer.open(bb);
      valSerializer = serializationFactory.getSerializer(valClass);
      valSerializer.open(bb);
      // counters
      mapOutputByteCounter = reporter.getCounter(MAP_OUTPUT_BYTES);
      mapOutputRecordCounter = reporter.getCounter(MAP_OUTPUT_RECORDS);
      Counters.Counter combineInputCounter = 
        reporter.getCounter(COMBINE_INPUT_RECORDS);
      combineOutputCounter = reporter.getCounter(COMBINE_OUTPUT_RECORDS);
      fileOutputByteCounter = reporter.getCounter(MAP_OUTPUT_MATERIALIZED_BYTES);
      // compression
      if (job.getCompressMapOutput()) {
        Class<? extends CompressionCodec> codecClass =
          job.getMapOutputCompressorClass(DefaultCodec.class);
        codec = ReflectionUtils.newInstance(codecClass, job);
      }
      // combiner
      combinerRunner = CombinerRunner.create(job, getTaskID(), 
                                             combineInputCounter,
                                             reporter, null);
      if (combinerRunner != null) {
        combineCollector= new CombineOutputCollector<K,V>(combineOutputCounter, reporter, conf);
      } else {
        combineCollector = null;
      }
      minSpillsForCombine = job.getInt("min.num.spills.for.combine", 3);
      spillThread.setDaemon(true);
      spillThread.setName("SpillThread");
      spillLock.lock();
      try {
        spillThread.start();
        while (!spillThreadRunning) {
          spillDone.await();
        }
      } catch (InterruptedException e) {
        throw (IOException)new IOException("Spill thread failed to initialize"
            ).initCause(sortSpillException);
      } finally {
        spillLock.unlock();
      }
      if (sortSpillException != null) {
        throw (IOException)new IOException("Spill thread failed to initialize"
            ).initCause(sortSpillException);
      }
    }

    public synchronized void collect(K key, V value, int partition
                                     ) throws IOException {
      reporter.progress();
      if (key.getClass() != keyClass) {
        throw new IOException("Type mismatch in key from map: expected "
                              + keyClass.getName() + ", recieved "
                              + key.getClass().getName());
      }
      if (value.getClass() != valClass) {
        throw new IOException("Type mismatch in value from map: expected "
                              + valClass.getName() + ", recieved "
                              + value.getClass().getName());
      }
      final int kvnext = (kvindex + 1) % kvoffsets.length;
      spillLock.lock();
      try {
        boolean kvfull;
        do {
          if (sortSpillException != null) {
            throw (IOException)new IOException("Spill failed"
                ).initCause(sortSpillException);
          }
          // sufficient acct space
          kvfull = kvnext == kvstart;
          final boolean kvsoftlimit = ((kvnext > kvend)
              ? kvnext - kvend > softRecordLimit
              : kvend - kvnext <= kvoffsets.length - softRecordLimit);
          if (kvstart == kvend && kvsoftlimit) {
            LOG.info("Spilling map output: record full = " + kvsoftlimit);
            startSpill();
          }
          if (kvfull) {
            try {
              while (kvstart != kvend) {
                reporter.progress();
                spillDone.await();
              }
            } catch (InterruptedException e) {
              throw (IOException)new IOException(
                  "Collector interrupted while waiting for the writer"
                  ).initCause(e);
            }
          }
        } while (kvfull);
      } finally {
        spillLock.unlock();
      }

      try {
        // serialize key bytes into buffer
        int keystart = bufindex;
        keySerializer.serialize(key);
        if (bufindex < keystart) {
          // wrapped the key; reset required
          bb.reset();
          keystart = 0;
        }
        // serialize value bytes into buffer
        final int valstart = bufindex;
        valSerializer.serialize(value);
        int valend = bb.markRecord();

        if (partition < 0 || partition >= partitions) {
          throw new IOException("Illegal partition for " + key + " (" +
              partition + ")");
        }

        mapOutputRecordCounter.increment(1);
        mapOutputByteCounter.increment(valend >= keystart
            ? valend - keystart
            : (bufvoid - keystart) + valend);

        // update accounting info
        int ind = kvindex * ACCTSIZE;
        kvoffsets[kvindex] = ind;
        kvindices[ind + PARTITION] = partition;
        kvindices[ind + KEYSTART] = keystart;
        kvindices[ind + VALSTART] = valstart;
        kvindex = kvnext;
      } catch (MapBufferTooSmallException e) {
        LOG.info("Record too large for in-memory buffer: " + e.getMessage());
        spillSingleRecord(key, value, partition);
        mapOutputRecordCounter.increment(1);
        return;
      }

    }

    /**
     * Compare logical range, st i, j MOD offset capacity.
     * Compare by partition, then by key.
     * @see IndexedSortable#compare
     */
    public int compare(int i, int j) {
      final int ii = kvoffsets[i % kvoffsets.length];
      final int ij = kvoffsets[j % kvoffsets.length];
      // sort by partition
      if (kvindices[ii + PARTITION] != kvindices[ij + PARTITION]) {
        return kvindices[ii + PARTITION] - kvindices[ij + PARTITION];
      }
      // sort by key
      return comparator.compare(kvbuffer,
          kvindices[ii + KEYSTART],
          kvindices[ii + VALSTART] - kvindices[ii + KEYSTART],
          kvbuffer,
          kvindices[ij + KEYSTART],
          kvindices[ij + VALSTART] - kvindices[ij + KEYSTART]);
    }

    /**
     * Swap logical indices st i, j MOD offset capacity.
     * @see IndexedSortable#swap
     */
    public void swap(int i, int j) {
      i %= kvoffsets.length;
      j %= kvoffsets.length;
      int tmp = kvoffsets[i];
      kvoffsets[i] = kvoffsets[j];
      kvoffsets[j] = tmp;
    }

    /**
     * Inner class managing the spill of serialized records to disk.
     */
    protected class BlockingBuffer extends DataOutputStream {

      public BlockingBuffer() {
        this(new Buffer());
      }

      private BlockingBuffer(OutputStream out) {
        super(out);
      }

      /**
       * Mark end of record. Note that this is required if the buffer is to
       * cut the spill in the proper place.
       */
      public int markRecord() {
        bufmark = bufindex;
        return bufindex;
      }

      /**
       * Set position from last mark to end of writable buffer, then rewrite
       * the data between last mark and kvindex.
       * This handles a special case where the key wraps around the buffer.
       * If the key is to be passed to a RawComparator, then it must be
       * contiguous in the buffer. This recopies the data in the buffer back
       * into itself, but starting at the beginning of the buffer. Note that
       * reset() should <b>only</b> be called immediately after detecting
       * this condition. To call it at any other time is undefined and would
       * likely result in data loss or corruption.
       * @see #markRecord()
       */
      protected synchronized void reset() throws IOException {
        // spillLock unnecessary; If spill wraps, then
        // bufindex < bufstart < bufend so contention is impossible
        // a stale value for bufstart does not affect correctness, since
        // we can only get false negatives that force the more
        // conservative path
        int headbytelen = bufvoid - bufmark;
        bufvoid = bufmark;
        if (bufindex + headbytelen < bufstart) {
          System.arraycopy(kvbuffer, 0, kvbuffer, headbytelen, bufindex);
          System.arraycopy(kvbuffer, bufvoid, kvbuffer, 0, headbytelen);
          bufindex += headbytelen;
        } else {
          byte[] keytmp = new byte[bufindex];
          System.arraycopy(kvbuffer, 0, keytmp, 0, bufindex);
          bufindex = 0;
          out.write(kvbuffer, bufmark, headbytelen);
          out.write(keytmp);
        }
      }
    }

    public class Buffer extends OutputStream {
      private final byte[] scratch = new byte[1];

      @Override
      public synchronized void write(int v)
          throws IOException {
        scratch[0] = (byte)v;
        write(scratch, 0, 1);
      }

      /**
       * Attempt to write a sequence of bytes to the collection buffer.
       * This method will block if the spill thread is running and it
       * cannot write.
       * @throws MapBufferTooSmallException if record is too large to
       *    deserialize into the collection buffer.
       */
      @Override
      public synchronized void write(byte b[], int off, int len)
          throws IOException {
        boolean buffull = false;
        boolean wrap = false;
        spillLock.lock();
        try {
          do {
            if (sortSpillException != null) {
              throw (IOException)new IOException("Spill failed"
                  ).initCause(sortSpillException);
            }

            // sufficient buffer space?
            if (bufstart <= bufend && bufend <= bufindex) {
              buffull = bufindex + len > bufvoid;
              wrap = (bufvoid - bufindex) + bufstart > len;
            } else {
              // bufindex <= bufstart <= bufend
              // bufend <= bufindex <= bufstart
              wrap = false;
              buffull = bufindex + len > bufstart;
            }

            if (kvstart == kvend) {
              // spill thread not running
              if (kvend != kvindex) {
                // we have records we can spill
                final boolean bufsoftlimit = (bufindex > bufend)
                  ? bufindex - bufend > softBufferLimit
                  : bufend - bufindex < bufvoid - softBufferLimit;
                if (bufsoftlimit || (buffull && !wrap)) {
                  LOG.info("Spilling map output: buffer full= " + bufsoftlimit);
                  startSpill();
                }
              } else if (buffull && !wrap) {
                // We have no buffered records, and this record is too large
                // to write into kvbuffer. We must spill it directly from
                // collect
                final int size = ((bufend <= bufindex)
                  ? bufindex - bufend
                  : (bufvoid - bufend) + bufindex) + len;
                bufstart = bufend = bufindex = bufmark = 0;
                kvstart = kvend = kvindex = 0;
                bufvoid = kvbuffer.length;
                throw new MapBufferTooSmallException(size + " bytes");
              }
            }

            if (buffull && !wrap) {
              try {
                while (kvstart != kvend) {
                  reporter.progress();
                  spillDone.await();
                }
              } catch (InterruptedException e) {
                  throw (IOException)new IOException(
                      "Buffer interrupted while waiting for the writer"
                      ).initCause(e);
              }
            }
          } while (buffull && !wrap);
        } finally {
          spillLock.unlock();
        }
        // here, we know that we have sufficient space to write
        if (buffull) {
          final int gaplen = bufvoid - bufindex;
          System.arraycopy(b, off, kvbuffer, bufindex, gaplen);
          len -= gaplen;
          off += gaplen;
          bufindex = 0;
        }
        System.arraycopy(b, off, kvbuffer, bufindex, len);
        bufindex += len;
      }
    }

    public synchronized void flush() throws IOException, ClassNotFoundException,
                                            InterruptedException {
      LOG.info("Starting flush of map output");
      spillLock.lock();
      try {
        while (kvstart != kvend) {
          reporter.progress();
          spillDone.await();
        }
        if (sortSpillException != null) {
          throw (IOException)new IOException("Spill failed"
              ).initCause(sortSpillException);
        }
        if (kvend != kvindex) {
          kvend = kvindex;
          bufend = bufmark;
          sortAndSpill();
        }
      } catch (InterruptedException e) {
        throw (IOException)new IOException(
            "Buffer interrupted while waiting for the writer"
            ).initCause(e);
      } finally {
        spillLock.unlock();
      }
      assert !spillLock.isHeldByCurrentThread();
      // shut down spill thread and wait for it to exit. Since the preceding
      // ensures that it is finished with its work (and sortAndSpill did not
      // throw), we elect to use an interrupt instead of setting a flag.
      // Spilling simultaneously from this thread while the spill thread
      // finishes its work might be both a useful way to extend this and also
      // sufficient motivation for the latter approach.
      try {
        spillThread.interrupt();
        spillThread.join();
      } catch (InterruptedException e) {
        throw (IOException)new IOException("Spill failed"
            ).initCause(e);
      }
      // release sort buffer before the merge
      kvbuffer = null;
      mergeParts();
      Path outputPath = mapOutputFile.getOutputFile();
      fileOutputByteCounter.increment(rfs.getFileStatus(outputPath).getLen());
    }

    public void close() { }

    protected class SpillThread extends Thread {

      @Override
      public void run() {
        spillLock.lock();
        spillThreadRunning = true;
        try {
          while (true) {
            spillDone.signal();
            while (kvstart == kvend) {
              spillReady.await();
            }
            try {
              spillLock.unlock();
              sortAndSpill();
            } catch (Exception e) {
              sortSpillException = e;
            } catch (Throwable t) {
              sortSpillException = t;
              String logMsg = "Task " + getTaskID() + " failed : " 
                              + StringUtils.stringifyException(t);
              reportFatalError(getTaskID(), t, logMsg);
            } finally {
              spillLock.lock();
              if (bufend < bufindex && bufindex < bufstart) {
                bufvoid = kvbuffer.length;
              }
              kvstart = kvend;
              bufstart = bufend;
            }
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          spillLock.unlock();
          spillThreadRunning = false;
        }
      }
    }

    private synchronized void startSpill() {
      LOG.info("bufstart = " + bufstart + "; bufend = " + bufmark +
               "; bufvoid = " + bufvoid);
      LOG.info("kvstart = " + kvstart + "; kvend = " + kvindex +
               "; length = " + kvoffsets.length);
      kvend = kvindex;
      bufend = bufmark;
      spillReady.signal();
    }

    private void sortAndSpill() throws IOException, ClassNotFoundException,
                                       InterruptedException {
      //approximate the length of the output file to be the length of the
      //buffer + header lengths for the partitions
      long size = (bufend >= bufstart
          ? bufend - bufstart
          : (bufvoid - bufend) + bufstart) +
                  partitions * APPROX_HEADER_LENGTH;
      FSDataOutputStream out = null;
      try {
        // create spill file
        final SpillRecord spillRec = new SpillRecord(partitions);
        final Path filename =
            mapOutputFile.getSpillFileForWrite(numSpills, size);
        out = rfs.create(filename);

        final int endPosition = (kvend > kvstart)
          ? kvend
          : kvoffsets.length + kvend;
        sorter.sort(MapOutputBuffer.this, kvstart, endPosition, reporter);
        int spindex = kvstart;
        IndexRecord rec = new IndexRecord();
        InMemValBytes value = new InMemValBytes();
        for (int i = 0; i < partitions; ++i) {
          IFile.Writer<K, V> writer = null;
          try {
            long segmentStart = out.getPos();
            writer = new Writer<K, V>(job, out, keyClass, valClass, codec,
                                      spilledRecordsCounter);
            if (combinerRunner == null) {
              // spill directly
              DataInputBuffer key = new DataInputBuffer();
              while (spindex < endPosition &&
                  kvindices[kvoffsets[spindex % kvoffsets.length]
                            + PARTITION] == i) {
                final int kvoff = kvoffsets[spindex % kvoffsets.length];
                getVBytesForOffset(kvoff, value);
                key.reset(kvbuffer, kvindices[kvoff + KEYSTART],
                          (kvindices[kvoff + VALSTART] - 
                           kvindices[kvoff + KEYSTART]));
                writer.append(key, value);
                ++spindex;
              }
            } else {
              int spstart = spindex;
              while (spindex < endPosition &&
                  kvindices[kvoffsets[spindex % kvoffsets.length]
                            + PARTITION] == i) {
                ++spindex;
              }
              // Note: we would like to avoid the combiner if we've fewer
              // than some threshold of records for a partition
              if (spstart != spindex) {
                combineCollector.setWriter(writer);
                RawKeyValueIterator kvIter =
                  new MRResultIterator(spstart, spindex);
                combinerRunner.combine(kvIter, combineCollector);
              }
            }

            // close the writer
            writer.close();

            // record offsets
            rec.startOffset = segmentStart;
            rec.rawLength = writer.getRawLength();
            rec.partLength = writer.getCompressedLength();
            spillRec.putIndex(rec, i);

            writer = null;
          } finally {
            if (null != writer) writer.close();
          }
        }

        if (totalIndexCacheMemory >= INDEX_CACHE_MEMORY_LIMIT) {
          // create spill index file
          Path indexFilename =
              mapOutputFile.getSpillIndexFileForWrite(numSpills, partitions
                  * MAP_OUTPUT_INDEX_RECORD_LENGTH);
          spillRec.writeToFile(indexFilename, job);
        } else {
          indexCacheList.add(spillRec);
          totalIndexCacheMemory +=
            spillRec.size() * MAP_OUTPUT_INDEX_RECORD_LENGTH;
        }
        LOG.info("Finished spill " + numSpills);
        ++numSpills;
      } finally {
        if (out != null) out.close();
      }
    }

    /**
     * Handles the degenerate case where serialization fails to fit in
     * the in-memory buffer, so we must spill the record from collect
     * directly to a spill file. Consider this "losing".
     */
    private void spillSingleRecord(final K key, final V value,
                                   int partition) throws IOException {
      long size = kvbuffer.length + partitions * APPROX_HEADER_LENGTH;
      FSDataOutputStream out = null;
      try {
        // create spill file
        final SpillRecord spillRec = new SpillRecord(partitions);
        final Path filename =
            mapOutputFile.getSpillFileForWrite(numSpills, size);
        out = rfs.create(filename);
        
        // we don't run the combiner for a single record
        IndexRecord rec = new IndexRecord();
        for (int i = 0; i < partitions; ++i) {
          IFile.Writer<K, V> writer = null;
          try {
            long segmentStart = out.getPos();
            // Create a new codec, don't care!
            writer = new IFile.Writer<K,V>(job, out, keyClass, valClass, codec,
                                            spilledRecordsCounter);

            if (i == partition) {
              final long recordStart = out.getPos();
              writer.append(key, value);
              // Note that our map byte count will not be accurate with
              // compression
              mapOutputByteCounter.increment(out.getPos() - recordStart);
            }
            writer.close();

            // record offsets
            rec.startOffset = segmentStart;
            rec.rawLength = writer.getRawLength();
            rec.partLength = writer.getCompressedLength();
            spillRec.putIndex(rec, i);

            writer = null;
          } catch (IOException e) {
            if (null != writer) writer.close();
            throw e;
          }
        }
        if (totalIndexCacheMemory >= INDEX_CACHE_MEMORY_LIMIT) {
          // create spill index file
          Path indexFilename =
              mapOutputFile.getSpillIndexFileForWrite(numSpills, partitions
                  * MAP_OUTPUT_INDEX_RECORD_LENGTH);
          spillRec.writeToFile(indexFilename, job);
        } else {
          indexCacheList.add(spillRec);
          totalIndexCacheMemory +=
            spillRec.size() * MAP_OUTPUT_INDEX_RECORD_LENGTH;
        }
        ++numSpills;
      } finally {
        if (out != null) out.close();
      }
    }

    /**
     * Given an offset, populate vbytes with the associated set of
     * deserialized value bytes. Should only be called during a spill.
     */
    private void getVBytesForOffset(int kvoff, InMemValBytes vbytes) {
      final int nextindex = (kvoff / ACCTSIZE ==
                            (kvend - 1 + kvoffsets.length) % kvoffsets.length)
        ? bufend
        : kvindices[(kvoff + ACCTSIZE + KEYSTART) % kvindices.length];
      int vallen = (nextindex >= kvindices[kvoff + VALSTART])
        ? nextindex - kvindices[kvoff + VALSTART]
        : (bufvoid - kvindices[kvoff + VALSTART]) + nextindex;
      vbytes.reset(kvbuffer, kvindices[kvoff + VALSTART], vallen);
    }

    /**
     * Inner class wrapping valuebytes, used for appendRaw.
     */
    protected class InMemValBytes extends DataInputBuffer {
      private byte[] buffer;
      private int start;
      private int length;
            
      public void reset(byte[] buffer, int start, int length) {
        this.buffer = buffer;
        this.start = start;
        this.length = length;
        
        if (start + length > bufvoid) {
          this.buffer = new byte[this.length];
          final int taillen = bufvoid - start;
          System.arraycopy(buffer, start, this.buffer, 0, taillen);
          System.arraycopy(buffer, 0, this.buffer, taillen, length-taillen);
          this.start = 0;
        }
        
        super.reset(this.buffer, this.start, this.length);
      }
    }

    protected class MRResultIterator implements RawKeyValueIterator {
      private final DataInputBuffer keybuf = new DataInputBuffer();
      private final InMemValBytes vbytes = new InMemValBytes();
      private final int end;
      private int current;
      public MRResultIterator(int start, int end) {
        this.end = end;
        current = start - 1;
      }
      public boolean next() throws IOException {
        return ++current < end;
      }
      public DataInputBuffer getKey() throws IOException {
        final int kvoff = kvoffsets[current % kvoffsets.length];
        keybuf.reset(kvbuffer, kvindices[kvoff + KEYSTART],
                     kvindices[kvoff + VALSTART] - kvindices[kvoff + KEYSTART]);
        return keybuf;
      }
      public DataInputBuffer getValue() throws IOException {
        getVBytesForOffset(kvoffsets[current % kvoffsets.length], vbytes);
        return vbytes;
      }
      public Progress getProgress() {
        return null;
      }
      public void close() { }
    }

    private void mergeParts() throws IOException, InterruptedException, 
                                     ClassNotFoundException {
      // get the approximate size of the final output/index files
      long finalOutFileSize = 0;
      long finalIndexFileSize = 0;
      final Path[] filename = new Path[numSpills];
      final TaskAttemptID mapId = getTaskID();

      for(int i = 0; i < numSpills; i++) {
        filename[i] = mapOutputFile.getSpillFile(i);
        finalOutFileSize += rfs.getFileStatus(filename[i]).getLen();
      }
      if (numSpills == 1) { //the spill is the final output
        rfs.rename(filename[0],
            new Path(filename[0].getParent(), "file.out"));
        if (indexCacheList.size() == 0) {
          rfs.rename(mapOutputFile.getSpillIndexFile(0),
              new Path(filename[0].getParent(),"file.out.index"));
        } else {
          indexCacheList.get(0).writeToFile(
                new Path(filename[0].getParent(),"file.out.index"), job);
        }
        return;
      }

      // read in paged indices
      for (int i = indexCacheList.size(); i < numSpills; ++i) {
        Path indexFileName = mapOutputFile.getSpillIndexFile(i);
        indexCacheList.add(new SpillRecord(indexFileName, job, null));
      }

      //make correction in the length to include the sequence file header
      //lengths for each partition
      finalOutFileSize += partitions * APPROX_HEADER_LENGTH;
      finalIndexFileSize = partitions * MAP_OUTPUT_INDEX_RECORD_LENGTH;
      Path finalOutputFile =
          mapOutputFile.getOutputFileForWrite(finalOutFileSize);
      Path finalIndexFile =
          mapOutputFile.getOutputIndexFileForWrite(finalIndexFileSize);

      //The output stream for the final single output file
      FSDataOutputStream finalOut = rfs.create(finalOutputFile, true, 4096);

      if (numSpills == 0) {
        //create dummy files
        IndexRecord rec = new IndexRecord();
        SpillRecord sr = new SpillRecord(partitions);
        try {
          for (int i = 0; i < partitions; i++) {
            long segmentStart = finalOut.getPos();
            Writer<K, V> writer =
              new Writer<K, V>(job, finalOut, keyClass, valClass, codec, null);
            writer.close();
            rec.startOffset = segmentStart;
            rec.rawLength = writer.getRawLength();
            rec.partLength = writer.getCompressedLength();
            sr.putIndex(rec, i);
          }
          sr.writeToFile(finalIndexFile, job);
        } finally {
          finalOut.close();
        }
        return;
      }
      {
        IndexRecord rec = new IndexRecord();
        final SpillRecord spillRec = new SpillRecord(partitions);
        for (int parts = 0; parts < partitions; parts++) {
          //create the segments to be merged
          List<Segment<K,V>> segmentList =
            new ArrayList<Segment<K, V>>(numSpills);
          for(int i = 0; i < numSpills; i++) {
            IndexRecord indexRecord = indexCacheList.get(i).getIndex(parts);

            Segment<K,V> s =
              new Segment<K,V>(job, rfs, filename[i], indexRecord.startOffset,
                               indexRecord.partLength, codec, true);
            segmentList.add(i, s);

            if (LOG.isDebugEnabled()) {
              LOG.debug("MapId=" + mapId + " Reducer=" + parts +
                  "Spill =" + i + "(" + indexRecord.startOffset + "," +
                  indexRecord.rawLength + ", " + indexRecord.partLength + ")");
            }
          }

          //merge
          @SuppressWarnings("unchecked")
          RawKeyValueIterator kvIter = Merger.merge(job, rfs,
                         keyClass, valClass, codec,
                         segmentList, job.getInt("io.sort.factor", 100),
                         new Path(mapId.toString()),
                         job.getOutputKeyComparator(), reporter,
                         null, spilledRecordsCounter);

          //write merged output to disk
          long segmentStart = finalOut.getPos();
          Writer<K, V> writer =
              new Writer<K, V>(job, finalOut, keyClass, valClass, codec,
                               spilledRecordsCounter);
          if (combinerRunner == null || numSpills < minSpillsForCombine) {
            Merger.writeFile(kvIter, writer, reporter, job);
          } else {
            combineCollector.setWriter(writer);
            combinerRunner.combine(kvIter, combineCollector);
          }

          //close
          writer.close();

          // record offsets
          rec.startOffset = segmentStart;
          rec.rawLength = writer.getRawLength();
          rec.partLength = writer.getCompressedLength();
          spillRec.putIndex(rec, parts);
        }
        spillRec.writeToFile(finalIndexFile, job);
        finalOut.close();
        for(int i = 0; i < numSpills; i++) {
          rfs.delete(filename[i],true);
        }
      }
    }

  } // MapOutputBuffer
  
  class MapOutputBufferwSource<SK extends Object, K extends Object, V extends Object> 
  implements IndexedSortable {
    private final int partitions;
    private final JobConf job;
    private final TaskReporter reporter;
    private final Class<K> keyClass;
    private final Class<V> valClass;
    private final Class<SK> skeyClass;
    private final RawComparator<K> comparator;
    private final RawComparator<SK> comparator2;
    private final SerializationFactory serializationFactory;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valSerializer;
    private final Serializer<SK> skeySerializer;
    private final CombinerRunner<K,V> combinerRunner;
    private final CombineOutputCollector<K, V> combineCollector;
    
    // Compression for map-outputs
    private CompressionCodec codec = null;

    // k/v accounting
    private volatile int kvstart = 0;  // marks beginning of spill
    private volatile int kvend = 0;    // marks beginning of collectable
    private int kvindex = 0;           // marks end of collected
    private final int[] kvoffsets;     // indices into kvindices
    private final int[] kvindices;     // partition, k/v offsets into kvbuffer
    private volatile int bufstart = 0; // marks beginning of spill
    private volatile int bufend = 0;   // marks beginning of collectable
    private volatile int bufvoid = 0;  // marks the point where we should stop
                                       // reading at the end of the buffer
    private int bufindex = 0;          // marks end of collected
    private int bufmark = 0;           // marks end of record
    private byte[] kvbuffer;           // main output buffer
    private static final int PARTITION = 0; // partition offset in acct
    private static final int KEYSTART = 1;  // key offset in acct
    private static final int SKEYSTART = 2;  // val offset in acct
    private static final int VALSTART = 3;  // val offset in acct
    private static final int ACCTSIZE = 4;  // total #fields in acct
    private static final int RECSIZE =
                       (ACCTSIZE + 1) * 4;  // acct bytes per record

    // spill accounting
    private volatile int numSpills = 0;
    private volatile Throwable sortSpillException = null;
    private final int softRecordLimit;
    private final int softBufferLimit;
    private final int minSpillsForCombine;
    private final IndexedSorter sorter;
    private final ReentrantLock spillLock = new ReentrantLock();
    private final Condition spillDone = spillLock.newCondition();
    private final Condition spillReady = spillLock.newCondition();
    private final BlockingBuffer bb = new BlockingBuffer();
    private volatile boolean spillThreadRunning = false;
    private final SpillThread spillThread = new SpillThread();

    private final FileSystem localFs;
    private final FileSystem rfs;
   
    private final Counters.Counter mapOutputByteCounter;
    private final Counters.Counter mapOutputRecordCounter;
    private final Counters.Counter combineOutputCounter;
    private final Counters.Counter fileOutputByteCounter;
    
    private ArrayList<SpillRecord> indexCacheList;
    private int totalIndexCacheMemory;
    private static final int INDEX_CACHE_MEMORY_LIMIT = 1024 * 1024;

    @SuppressWarnings("unchecked")
    public MapOutputBufferwSource(TaskUmbilicalProtocol umbilical, JobConf job,
                           TaskReporter reporter
                           ) throws IOException, ClassNotFoundException {
      this.job = job;
      this.reporter = reporter;
      localFs = FileSystem.getLocal(job);
      partitions = job.getNumReduceTasks();
       
      rfs = ((LocalFileSystem)localFs).getRaw();

      indexCacheList = new ArrayList<SpillRecord>();
      
      //sanity checks
      final float spillper = job.getFloat("io.sort.spill.percent",(float)0.8);
      final float recper = job.getFloat("io.sort.record.percent",(float)0.05);
      final int sortmb = job.getInt("io.sort.mb", 100);
      if (spillper > (float)1.0 || spillper < (float)0.0) {
        throw new IOException("Invalid \"io.sort.spill.percent\": " + spillper);
      }
      if (recper > (float)1.0 || recper < (float)0.01) {
        throw new IOException("Invalid \"io.sort.record.percent\": " + recper);
      }
      if ((sortmb & 0x7FF) != sortmb) {
        throw new IOException("Invalid \"io.sort.mb\": " + sortmb);
      }
      sorter = ReflectionUtils.newInstance(
            job.getClass("map.sort.class", QuickSort.class, IndexedSorter.class), job);
      LOG.info("io.sort.mb = " + sortmb);
      // buffers and accounting
      int maxMemUsage = sortmb << 20;
      int recordCapacity = (int)(maxMemUsage * recper);
      recordCapacity -= recordCapacity % RECSIZE;
      kvbuffer = new byte[maxMemUsage - recordCapacity];
      bufvoid = kvbuffer.length;
      recordCapacity /= RECSIZE;
      kvoffsets = new int[recordCapacity];
      kvindices = new int[recordCapacity * ACCTSIZE];
      softBufferLimit = (int)(kvbuffer.length * spillper);
      softRecordLimit = (int)(kvoffsets.length * spillper);
      LOG.info("data buffer = " + softBufferLimit + "/" + kvbuffer.length);
      LOG.info("record buffer = " + softRecordLimit + "/" + kvoffsets.length);
      // k/v serialization
      comparator = job.getOutputKeyComparator();
      comparator2 = (RawComparator<SK>)WritableComparator.get(job.getStaticKeyClass().asSubclass(WritableComparable.class));
      keyClass = (Class<K>)job.getMapOutputKeyClass();
      valClass = (Class<V>)job.getMapOutputValueClass();
      skeyClass = (Class<SK>)job.getStaticKeyClass();
      serializationFactory = new SerializationFactory(job);
      keySerializer = serializationFactory.getSerializer(keyClass);
      keySerializer.open(bb);
      valSerializer = serializationFactory.getSerializer(valClass);
      valSerializer.open(bb);
      skeySerializer = serializationFactory.getSerializer(skeyClass);
      skeySerializer.open(bb);
      // counters
      mapOutputByteCounter = reporter.getCounter(MAP_OUTPUT_BYTES);
      mapOutputRecordCounter = reporter.getCounter(MAP_OUTPUT_RECORDS);
      Counters.Counter combineInputCounter = 
        reporter.getCounter(COMBINE_INPUT_RECORDS);
      combineOutputCounter = reporter.getCounter(COMBINE_OUTPUT_RECORDS);
      fileOutputByteCounter = reporter.getCounter(MAP_OUTPUT_MATERIALIZED_BYTES);
      // compression
      if (job.getCompressMapOutput()) {
        Class<? extends CompressionCodec> codecClass =
          job.getMapOutputCompressorClass(DefaultCodec.class);
        codec = ReflectionUtils.newInstance(codecClass, job);
      }
      // combiner
      combinerRunner = CombinerRunner.create(job, getTaskID(), 
                                             combineInputCounter,
                                             reporter, null);
      if (combinerRunner != null) {
        combineCollector= new CombineOutputCollector<K,V>(combineOutputCounter, reporter, conf);
      } else {
        combineCollector = null;
      }
      minSpillsForCombine = job.getInt("min.num.spills.for.combine", 3);
      spillThread.setDaemon(true);
      spillThread.setName("SpillThread");
      spillLock.lock();
      try {
        spillThread.start();
        while (!spillThreadRunning) {
          spillDone.await();
        }
      } catch (InterruptedException e) {
        throw (IOException)new IOException("Spill thread failed to initialize"
            ).initCause(sortSpillException);
      } finally {
        spillLock.unlock();
      }
      if (sortSpillException != null) {
        throw (IOException)new IOException("Spill thread failed to initialize"
            ).initCause(sortSpillException);
      }
    }

    public synchronized void collect(SK skey, K key, V value, int partition
                                     ) throws IOException, InterruptedException {
      reporter.progress();
      if (key.getClass() != keyClass) {
        throw new IOException("Type mismatch in key from map: expected "
                              + keyClass.getName() + ", recieved "
                              + key.getClass().getName());
      }
      if (key.getClass() != skeyClass) {
          throw new IOException("Type mismatch in skey from map: expected "
                                + skeyClass.getName() + ", recieved "
                                + key.getClass().getName());
        }
      
      if (value.getClass() != valClass) {
        throw new IOException("Type mismatch in value from map: expected "
                              + valClass.getName() + ", recieved "
                              + value.getClass().getName());
      }
      final int kvnext = (kvindex + 1) % kvoffsets.length;
      spillLock.lock();
      try {
        boolean kvfull;
        do {
          if (sortSpillException != null) {
            throw (IOException)new IOException("Spill failed"
                ).initCause(sortSpillException);
          }
          // sufficient acct space
          kvfull = kvnext == kvstart;
          final boolean kvsoftlimit = ((kvnext > kvend)
              ? kvnext - kvend > softRecordLimit
              : kvend - kvnext <= kvoffsets.length - softRecordLimit);
          if (kvstart == kvend && kvsoftlimit) {
            LOG.info("Spilling map output: record full = " + kvsoftlimit);
            startSpill();
          }
          if (kvfull) {
            try {
              while (kvstart != kvend) {
                reporter.progress();
                spillDone.await();
              }
            } catch (InterruptedException e) {
              throw (IOException)new IOException(
                  "Collector interrupted while waiting for the writer"
                  ).initCause(e);
            }
          }
        } while (kvfull);
      } finally {
        spillLock.unlock();
      }

      try {
        // serialize key bytes into buffer
        int keystart = bufindex;
        keySerializer.serialize(key);
        if (bufindex < keystart) {
          // wrapped the key; reset required
          bb.reset();
          keystart = 0;
        }
        // serialize source key bytes into buffer
        int skeystart = bufindex;
        skeySerializer.serialize(skey);
        if (bufindex < skeystart) {
            // wrapped the key; reset required
            bb.reset();
            skeystart = 0;
        }
        // serialize value bytes into buffer
        int valstart = bufindex;
        valSerializer.serialize(value);

        int recend = bb.markRecord();

        if (partition < 0 || partition >= partitions) {
          throw new IOException("Illegal partition for " + key + " (" +
              partition + ")");
        }

        mapOutputRecordCounter.increment(1);
        mapOutputByteCounter.increment(recend >= keystart
            ? recend - keystart
            : (bufvoid - keystart) + recend);

        // update accounting info
        int ind = kvindex * ACCTSIZE;
        kvoffsets[kvindex] = ind;
        kvindices[ind + PARTITION] = partition;
        kvindices[ind + KEYSTART] = keystart;
        kvindices[ind + SKEYSTART] = skeystart;
        kvindices[ind + VALSTART] = valstart;
        kvindex = kvnext;
      } catch (MapBufferTooSmallException e) {
        LOG.info("Record too large for in-memory buffer: " + e.getMessage());
        spillSingleRecord(key, value, partition);
        mapOutputRecordCounter.increment(1);
        throw new IOException("Record too large for in-memory buffer: " + e.getMessage());
      }

    }

    /**
     * Compare logical range, st i, j MOD offset capacity.
     * Compare by partition, then by key.
     * @see IndexedSortable#compare
     */
    public int compare(int i, int j) {
      final int ii = kvoffsets[i % kvoffsets.length];
      final int ij = kvoffsets[j % kvoffsets.length];
      // sort by partition
      if (kvindices[ii + PARTITION] != kvindices[ij + PARTITION]) {
        return kvindices[ii + PARTITION] - kvindices[ij + PARTITION];
      }
      // sort by key and second key, which is source key
      
      //IGNORE the source key length, since we only consider the basis case, the source key is integer, long type
      //otherwise, if the source key is text, there will be a problem
      int cmpres = comparator.compare(kvbuffer,
	          kvindices[ii + KEYSTART],
	          kvindices[ii + SKEYSTART] - kvindices[ii + KEYSTART],
	          kvbuffer,
	          kvindices[ij + KEYSTART],
	          kvindices[ij + SKEYSTART] - kvindices[ij + KEYSTART]);
      
      if(cmpres == 0){
    	  //sort by source key
    	  return comparator2.compare(kvbuffer,
    	          kvindices[ii + SKEYSTART],
    	          kvindices[ii + VALSTART] - kvindices[ii + SKEYSTART],
    	          kvbuffer,
    	          kvindices[ij + SKEYSTART],
    	          kvindices[ij + VALSTART] - kvindices[ij + SKEYSTART]);
      }else{
    	  return cmpres;
      }
    }

    /**
     * Swap logical indices st i, j MOD offset capacity.
     * @see IndexedSortable#swap
     */
    public void swap(int i, int j) {
      i %= kvoffsets.length;
      j %= kvoffsets.length;
      int tmp = kvoffsets[i];
      kvoffsets[i] = kvoffsets[j];
      kvoffsets[j] = tmp;
    }

    /**
     * Inner class managing the spill of serialized records to disk.
     */
    protected class BlockingBuffer extends DataOutputStream {

      public BlockingBuffer() {
        this(new Buffer());
      }

      private BlockingBuffer(OutputStream out) {
        super(out);
      }

      /**
       * Mark end of record. Note that this is required if the buffer is to
       * cut the spill in the proper place.
       */
      public int markRecord() {
        bufmark = bufindex;
        return bufindex;
      }

      /**
       * Set position from last mark to end of writable buffer, then rewrite
       * the data between last mark and kvindex.
       * This handles a special case where the key wraps around the buffer.
       * If the key is to be passed to a RawComparator, then it must be
       * contiguous in the buffer. This recopies the data in the buffer back
       * into itself, but starting at the beginning of the buffer. Note that
       * reset() should <b>only</b> be called immediately after detecting
       * this condition. To call it at any other time is undefined and would
       * likely result in data loss or corruption.
       * @see #markRecord()
       */
      protected synchronized void reset() throws IOException {
        // spillLock unnecessary; If spill wraps, then
        // bufindex < bufstart < bufend so contention is impossible
        // a stale value for bufstart does not affect correctness, since
        // we can only get false negatives that force the more
        // conservative path
        int headbytelen = bufvoid - bufmark;
        bufvoid = bufmark;
        if (bufindex + headbytelen < bufstart) {
          System.arraycopy(kvbuffer, 0, kvbuffer, headbytelen, bufindex);
          System.arraycopy(kvbuffer, bufvoid, kvbuffer, 0, headbytelen);
          bufindex += headbytelen;
        } else {
          byte[] keytmp = new byte[bufindex];
          System.arraycopy(kvbuffer, 0, keytmp, 0, bufindex);
          bufindex = 0;
          out.write(kvbuffer, bufmark, headbytelen);
          out.write(keytmp);
        }
      }
    }

    public class Buffer extends OutputStream {
      private final byte[] scratch = new byte[1];

      @Override
      public synchronized void write(int v)
          throws IOException {
        scratch[0] = (byte)v;
        write(scratch, 0, 1);
      }

      /**
       * Attempt to write a sequence of bytes to the collection buffer.
       * This method will block if the spill thread is running and it
       * cannot write.
       * @throws MapBufferTooSmallException if record is too large to
       *    deserialize into the collection buffer.
       */
      @Override
      public synchronized void write(byte b[], int off, int len)
          throws IOException {
        boolean buffull = false;
        boolean wrap = false;
        spillLock.lock();
        try {
          do {
            if (sortSpillException != null) {
              throw (IOException)new IOException("Spill failed"
                  ).initCause(sortSpillException);
            }

            // sufficient buffer space?
            if (bufstart <= bufend && bufend <= bufindex) {
              buffull = bufindex + len > bufvoid;
              wrap = (bufvoid - bufindex) + bufstart > len;
            } else {
              // bufindex <= bufstart <= bufend
              // bufend <= bufindex <= bufstart
              wrap = false;
              buffull = bufindex + len > bufstart;
            }

            if (kvstart == kvend) {
              // spill thread not running
              if (kvend != kvindex) {
                // we have records we can spill
                final boolean bufsoftlimit = (bufindex > bufend)
                  ? bufindex - bufend > softBufferLimit
                  : bufend - bufindex < bufvoid - softBufferLimit;
                if (bufsoftlimit || (buffull && !wrap)) {
                  LOG.info("Spilling map output: buffer full= " + bufsoftlimit);
                  startSpill();
                }
              } else if (buffull && !wrap) {
                // We have no buffered records, and this record is too large
                // to write into kvbuffer. We must spill it directly from
                // collect
                final int size = ((bufend <= bufindex)
                  ? bufindex - bufend
                  : (bufvoid - bufend) + bufindex) + len;
                bufstart = bufend = bufindex = bufmark = 0;
                kvstart = kvend = kvindex = 0;
                bufvoid = kvbuffer.length;
                throw new MapBufferTooSmallException(size + " bytes");
              }
            }

            if (buffull && !wrap) {
              try {
                while (kvstart != kvend) {
                  reporter.progress();
                  spillDone.await();
                }
              } catch (InterruptedException e) {
                  throw (IOException)new IOException(
                      "Buffer interrupted while waiting for the writer"
                      ).initCause(e);
              }
            }
          } while (buffull && !wrap);
        } finally {
          spillLock.unlock();
        }
        // here, we know that we have sufficient space to write
        if (buffull) {
          final int gaplen = bufvoid - bufindex;
          System.arraycopy(b, off, kvbuffer, bufindex, gaplen);
          len -= gaplen;
          off += gaplen;
          bufindex = 0;
        }
        System.arraycopy(b, off, kvbuffer, bufindex, len);
        bufindex += len;
      }
    }

    public synchronized void flush() throws IOException, ClassNotFoundException,
                                            InterruptedException {
      LOG.info("Starting flush of map output");
      spillLock.lock();
      try {
        while (kvstart != kvend) {
          reporter.progress();
          spillDone.await();
        }
        if (sortSpillException != null) {
          throw (IOException)new IOException("Spill failed"
              ).initCause(sortSpillException);
        }
        if (kvend != kvindex) {
          kvend = kvindex;
          bufend = bufmark;
          sortAndSpill();
        }
      } catch (InterruptedException e) {
        throw (IOException)new IOException(
            "Buffer interrupted while waiting for the writer"
            ).initCause(e);
      } finally {
        spillLock.unlock();
      }
      assert !spillLock.isHeldByCurrentThread();
      // shut down spill thread and wait for it to exit. Since the preceding
      // ensures that it is finished with its work (and sortAndSpill did not
      // throw), we elect to use an interrupt instead of setting a flag.
      // Spilling simultaneously from this thread while the spill thread
      // finishes its work might be both a useful way to extend this and also
      // sufficient motivation for the latter approach.
      try {
        spillThread.interrupt();
        spillThread.join();
      } catch (InterruptedException e) {
        throw (IOException)new IOException("Spill failed"
            ).initCause(e);
      }
      // release sort buffer before the merge
      kvbuffer = null;
      mergeParts();
      Path outputPath = mapOutputFile.getOutputFile();
      fileOutputByteCounter.increment(rfs.getFileStatus(outputPath).getLen());
    }

    public void close() { }

    protected class SpillThread extends Thread {

      @Override
      public void run() {
        spillLock.lock();
        spillThreadRunning = true;
        try {
          while (true) {
            spillDone.signal();
            while (kvstart == kvend) {
              spillReady.await();
            }
            try {
              spillLock.unlock();
              sortAndSpill();
            } catch (Exception e) {
              sortSpillException = e;
            } catch (Throwable t) {
              sortSpillException = t;
              String logMsg = "Task " + getTaskID() + " failed : " 
                              + StringUtils.stringifyException(t);
              reportFatalError(getTaskID(), t, logMsg);
            } finally {
              spillLock.lock();
              if (bufend < bufindex && bufindex < bufstart) {
                bufvoid = kvbuffer.length;
              }
              kvstart = kvend;
              bufstart = bufend;
            }
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          spillLock.unlock();
          spillThreadRunning = false;
        }
      }
    }

    private synchronized void startSpill() {
      LOG.info("bufstart = " + bufstart + "; bufend = " + bufmark +
               "; bufvoid = " + bufvoid);
      LOG.info("kvstart = " + kvstart + "; kvend = " + kvindex +
               "; length = " + kvoffsets.length);
      kvend = kvindex;
      bufend = bufmark;
      spillReady.signal();
    }

    private void sortAndSpill() throws IOException, ClassNotFoundException,
                                       InterruptedException {
      //approximate the length of the output file to be the length of the
      //buffer + header lengths for the partitions
      long size = (bufend >= bufstart
          ? bufend - bufstart
          : (bufvoid - bufend) + bufstart) +
                  partitions * APPROX_HEADER_LENGTH;
      FSDataOutputStream out = null;
      try {
        // create spill file
        final SpillRecord spillRec = new SpillRecord(partitions);
        final Path filename =
            mapOutputFile.getSpillFileForWrite(numSpills, size);
        out = rfs.create(filename);

        final int endPosition = (kvend > kvstart)
          ? kvend
          : kvoffsets.length + kvend;
        sorter.sort(MapOutputBufferwSource.this, kvstart, endPosition, reporter);
        int spindex = kvstart;
        IndexRecord rec = new IndexRecord();
        InMemValBytes skey = new InMemValBytes();
        InMemValBytes value = new InMemValBytes();
        for (int i = 0; i < partitions; ++i) {
          IFile.TrippleWriter<K, V, SK> writer = null;
          try {
            long segmentStart = out.getPos();
            writer = new IFile.TrippleWriter<K, V, SK>(job, out, keyClass, valClass, skeyClass, codec,
                                      spilledRecordsCounter);
            if (combinerRunner == null) {
              // spill directly
              DataInputBuffer key = new DataInputBuffer();
              while (spindex < endPosition &&
                  kvindices[kvoffsets[spindex % kvoffsets.length]
                            + PARTITION] == i) {
                final int kvoff = kvoffsets[spindex % kvoffsets.length];
                //getSKBytesForOffset(kvoff, skey);
                getVBytesForOffset(kvoff, value);
                skey.reset(kvbuffer, kvindices[kvoff + SKEYSTART],
                        (kvindices[kvoff + VALSTART] - 
                         kvindices[kvoff + SKEYSTART]));
                key.reset(kvbuffer, kvindices[kvoff + KEYSTART],
                          (kvindices[kvoff + SKEYSTART] - 
                           kvindices[kvoff + KEYSTART]));
                writer.append(key, value, skey);
                
                ++spindex;
              }
            } else {
              int spstart = spindex;
              while (spindex < endPosition &&
                  kvindices[kvoffsets[spindex % kvoffsets.length]
                            + PARTITION] == i) {
                ++spindex;
              }
              // Note: we would like to avoid the combiner if we've fewer
              // than some threshold of records for a partition
              
              if (spstart != spindex) {
            	  throw new IOException("we don't consider this");
            	 /*
                combineCollector.setWriter(writer);
                RawKeyValueIterator kvIter =
                  new MRResultIterator(spstart, spindex);
                combinerRunner.combine(kvIter, combineCollector);
                */
              }
              
            }

            // close the writer
            writer.close();

            // record offsets
            rec.startOffset = segmentStart;
            rec.rawLength = writer.getRawLength();
            rec.partLength = writer.getCompressedLength();
            spillRec.putIndex(rec, i);

            writer = null;
          } finally {
            if (null != writer) writer.close();
          }
        }

        if (totalIndexCacheMemory >= INDEX_CACHE_MEMORY_LIMIT) {
          // create spill index file
          Path indexFilename =
              mapOutputFile.getSpillIndexFileForWrite(numSpills, partitions
                  * MAP_OUTPUT_INDEX_RECORD_LENGTH);
          spillRec.writeToFile(indexFilename, job);
        } else {
          indexCacheList.add(spillRec);
          totalIndexCacheMemory +=
            spillRec.size() * MAP_OUTPUT_INDEX_RECORD_LENGTH;
        }
        LOG.info("Finished spill " + numSpills);
        ++numSpills;
      } finally {
        if (out != null) out.close();
      }
    }

    /**
     * Handles the degenerate case where serialization fails to fit in
     * the in-memory buffer, so we must spill the record from collect
     * directly to a spill file. Consider this "losing".
     */
    private void spillSingleRecord(final K key, final V value,
                                   int partition) throws IOException {
      long size = kvbuffer.length + partitions * APPROX_HEADER_LENGTH;
      FSDataOutputStream out = null;
      try {
        // create spill file
        final SpillRecord spillRec = new SpillRecord(partitions);
        final Path filename =
            mapOutputFile.getSpillFileForWrite(numSpills, size);
        out = rfs.create(filename);
        
        // we don't run the combiner for a single record
        IndexRecord rec = new IndexRecord();
        for (int i = 0; i < partitions; ++i) {
          IFile.Writer<K, V> writer = null;
          try {
            long segmentStart = out.getPos();
            // Create a new codec, don't care!
            writer = new IFile.Writer<K,V>(job, out, keyClass, valClass, codec,
                                            spilledRecordsCounter);

            if (i == partition) {
              final long recordStart = out.getPos();
              writer.append(key, value);
              // Note that our map byte count will not be accurate with
              // compression
              mapOutputByteCounter.increment(out.getPos() - recordStart);
            }
            writer.close();

            // record offsets
            rec.startOffset = segmentStart;
            rec.rawLength = writer.getRawLength();
            rec.partLength = writer.getCompressedLength();
            spillRec.putIndex(rec, i);

            writer = null;
          } catch (IOException e) {
            if (null != writer) writer.close();
            throw e;
          }
        }
        if (totalIndexCacheMemory >= INDEX_CACHE_MEMORY_LIMIT) {
          // create spill index file
          Path indexFilename =
              mapOutputFile.getSpillIndexFileForWrite(numSpills, partitions
                  * MAP_OUTPUT_INDEX_RECORD_LENGTH);
          spillRec.writeToFile(indexFilename, job);
        } else {
          indexCacheList.add(spillRec);
          totalIndexCacheMemory +=
            spillRec.size() * MAP_OUTPUT_INDEX_RECORD_LENGTH;
        }
        ++numSpills;
      } finally {
        if (out != null) out.close();
      }
    }

    /**
     * Given an offset, populate vbytes with the associated set of
     * deserialized value bytes. Should only be called during a spill.
     */
    
    private void getVBytesForOffset(int kvoff, InMemValBytes vbytes) {
      final int nextindex = (kvoff / ACCTSIZE ==
                            (kvend - 1 + kvoffsets.length) % kvoffsets.length)
        ? bufend
        : kvindices[(kvoff + ACCTSIZE + KEYSTART) % kvindices.length];
      int vallen = (nextindex >= kvindices[kvoff + VALSTART])
        ? nextindex - kvindices[kvoff + VALSTART]
        : (bufvoid - kvindices[kvoff + VALSTART]) + nextindex;
      vbytes.reset(kvbuffer, kvindices[kvoff + VALSTART], vallen);
    }
	
    private void getSKBytesForOffset(int kvoff, InMemValBytes skbytes) {
        final int nextindex = (kvoff / ACCTSIZE ==
                              (kvend - 1 + kvoffsets.length) % kvoffsets.length)
          ? bufend
          : kvindices[(kvoff + ACCTSIZE + VALSTART) % kvindices.length];
        int skeylen = (nextindex >= kvindices[kvoff + SKEYSTART])
          ? nextindex - kvindices[kvoff + SKEYSTART]
          : (bufvoid - kvindices[kvoff + SKEYSTART]) + nextindex;
          skbytes.reset(kvbuffer, kvindices[kvoff + SKEYSTART], skeylen);
      }
    
    /**
     * Inner class wrapping valuebytes, used for appendRaw.
     */
    protected class InMemValBytes extends DataInputBuffer {
      private byte[] buffer;
      private int start;
      private int length;
            
      public void reset(byte[] buffer, int start, int length) {
        this.buffer = buffer;
        this.start = start;
        this.length = length;
        
        if (start + length > bufvoid) {
          this.buffer = new byte[this.length];
          final int taillen = bufvoid - start;
          System.arraycopy(buffer, start, this.buffer, 0, taillen);
          System.arraycopy(buffer, 0, this.buffer, taillen, length-taillen);
          this.start = 0;
        }
        
        super.reset(this.buffer, this.start, this.length);
      }
    }

    protected class MRResultIterator implements RawKeyValueIterator {
      private final DataInputBuffer keybuf = new DataInputBuffer();
      private final InMemValBytes vbytes = new InMemValBytes();
      private final int end;
      private int current;
      public MRResultIterator(int start, int end) {
        this.end = end;
        current = start - 1;
      }
      public boolean next() throws IOException {
        return ++current < end;
      }
      public DataInputBuffer getKey() throws IOException {
        final int kvoff = kvoffsets[current % kvoffsets.length];
        keybuf.reset(kvbuffer, kvindices[kvoff + KEYSTART],
                     kvindices[kvoff + VALSTART] - kvindices[kvoff + KEYSTART]);
        return keybuf;
      }
      public DataInputBuffer getValue() throws IOException {
        getVBytesForOffset(kvoffsets[current % kvoffsets.length], vbytes);
        return vbytes;
      }
      public Progress getProgress() {
        return null;
      }
      public void close() { }
    }

    private void mergeParts() throws IOException, InterruptedException, 
                                     ClassNotFoundException {
      // get the approximate size of the final output/index files
      long finalOutFileSize = 0;
      long finalIndexFileSize = 0;
      final Path[] filename = new Path[numSpills];
      final TaskAttemptID mapId = getTaskID();

      for(int i = 0; i < numSpills; i++) {
        filename[i] = mapOutputFile.getSpillFile(i);
        finalOutFileSize += rfs.getFileStatus(filename[i]).getLen();
      }
      if (numSpills == 1) { //the spill is the final output
        rfs.rename(filename[0],
            new Path(filename[0].getParent(), "file.out"));
        if (indexCacheList.size() == 0) {
          rfs.rename(mapOutputFile.getSpillIndexFile(0),
              new Path(filename[0].getParent(),"file.out.index"));
        } else {
          indexCacheList.get(0).writeToFile(
                new Path(filename[0].getParent(),"file.out.index"), job);
        }
        return;
      }

      // read in paged indices
      for (int i = indexCacheList.size(); i < numSpills; ++i) {
        Path indexFileName = mapOutputFile.getSpillIndexFile(i);
        indexCacheList.add(new SpillRecord(indexFileName, job, null));
      }

      //make correction in the length to include the sequence file header
      //lengths for each partition
      finalOutFileSize += partitions * APPROX_HEADER_LENGTH;
      finalIndexFileSize = partitions * MAP_OUTPUT_INDEX_RECORD_LENGTH;
      Path finalOutputFile =
          mapOutputFile.getOutputFileForWrite(finalOutFileSize);
      Path finalIndexFile =
          mapOutputFile.getOutputIndexFileForWrite(finalIndexFileSize);

      //The output stream for the final single output file
      FSDataOutputStream finalOut = rfs.create(finalOutputFile, true, 4096);

      if (numSpills == 0) {
        //create dummy files
        IndexRecord rec = new IndexRecord();
        SpillRecord sr = new SpillRecord(partitions);
        try {
          for (int i = 0; i < partitions; i++) {
            long segmentStart = finalOut.getPos();
            Writer<K, V> writer =
              new Writer<K, V>(job, finalOut, keyClass, valClass, codec, null);
            writer.close();
            rec.startOffset = segmentStart;
            rec.rawLength = writer.getRawLength();
            rec.partLength = writer.getCompressedLength();
            sr.putIndex(rec, i);
          }
          sr.writeToFile(finalIndexFile, job);
        } finally {
          finalOut.close();
        }
        return;
      }
      {
        IndexRecord rec = new IndexRecord();
        final SpillRecord spillRec = new SpillRecord(partitions);
        for (int parts = 0; parts < partitions; parts++) {
          //create the segments to be merged
          List<KVSSegment<K,V,SK>> segmentList = new ArrayList<KVSSegment<K,V,SK>>(numSpills);
          for(int i = 0; i < numSpills; i++) {
            IndexRecord indexRecord = indexCacheList.get(i).getIndex(parts);

            KVSSegment<K,V,SK> s =
              new KVSSegment<K,V,SK>(job, rfs, filename[i], indexRecord.startOffset,
                               indexRecord.partLength, codec, true, -1);
            segmentList.add(i, s);

            if (LOG.isDebugEnabled()) {
              LOG.debug("MapId=" + mapId + " Reducer=" + parts +
                  "Spill =" + i + "(" + indexRecord.startOffset + "," +
                  indexRecord.rawLength + ", " + indexRecord.partLength + ")");
            }
          }

          final RawComparator<SK> comparator2 =
      	        (RawComparator<SK>)WritableComparator.get(job.getStaticKeyClass().asSubclass(WritableComparable.class));
          
          //merge
          @SuppressWarnings("unchecked")
          RawKeyValueSourceIterator kvIter = PreserveMerger.merge(job, rfs,
                         keyClass, valClass, skeyClass, codec,
                         segmentList, job.getInt("io.sort.factor", 100),
                         new Path(mapId.toString()),
                         job.getOutputKeyComparator(), comparator2, reporter,
                         null, spilledRecordsCounter);
          
          //write merged output to disk
          long segmentStart = finalOut.getPos();
          TrippleWriter<K, V, SK> writer =
              new TrippleWriter<K, V, SK>(job, finalOut, keyClass, valClass, skeyClass, codec,
                               spilledRecordsCounter);
          if (combinerRunner == null || numSpills < minSpillsForCombine) {
        	  PreserveMerger.writeFile(kvIter, writer, reporter, job);
          } else {
        	  throw new RuntimeException("not implemented!!");
            //combineCollector.setWriter(writer);
            //combinerRunner.combine(kvIter, combineCollector);
          }

          //close
          writer.close();

          // record offsets
          rec.startOffset = segmentStart;
          rec.rawLength = writer.getRawLength();
          rec.partLength = writer.getCompressedLength();
          spillRec.putIndex(rec, parts);
        }
        spillRec.writeToFile(finalIndexFile, job);
        finalOut.close();
        for(int i = 0; i < numSpills; i++) {
          rfs.delete(filename[i],true);
        }
      }
    }


  } // MapPreserveOutputBuffer
  
  /**
   * Exception indicating that the allocated sort buffer is insufficient
   * to hold the current record.
   */
  @SuppressWarnings("serial")
  private static class MapBufferTooSmallException extends IOException {
    public MapBufferTooSmallException(String s) {
      super(s);
    }
  }

}
