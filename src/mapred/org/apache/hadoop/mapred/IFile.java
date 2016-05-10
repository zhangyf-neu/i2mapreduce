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

import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.mortbay.log.Log;

/**
 * <code>IFile</code> is the simple <key-len, value-len, key, value> format for
 * the intermediate map-outputs in Map-Reduce.
 * 
 * There is a <code>Writer</code> to write out map-outputs in this format and a
 * <code>Reader</code> to read files of this format.
 */
public class IFile {

	private static final int EOF_MARKER = -1;

	/**
	 * <code>IFile.Writer</code> to write out intermediate map-outputs.
	 */
	public static class Writer<K extends Object, V extends Object> {
		FSDataOutputStream out;
		boolean ownOutputStream = false;
		long start = 0;
		FSDataOutputStream rawOut;

		CompressionOutputStream compressedOut;
		Compressor compressor;
		boolean compressOutput = false;

		long decompressedBytesWritten = 0;
		long compressedBytesWritten = 0;

		// Count records written to disk
		private long numRecordsWritten = 0;
		private final Counters.Counter writtenRecordsCounter;

		IFileOutputStream checksumOut;

		Class<K> keyClass;
		Class<V> valueClass;
		Serializer<K> keySerializer;
		Serializer<V> valueSerializer;

		DataOutputBuffer buffer = new DataOutputBuffer();

		public Writer(Configuration conf, FileSystem fs, Path file,
				Class<K> keyClass, Class<V> valueClass, CompressionCodec codec,
				Counters.Counter writesCounter) throws IOException {
			this(conf, fs.create(file), keyClass, valueClass, codec,
					writesCounter);
			ownOutputStream = true;
		}

		public Writer(Configuration conf, FSDataOutputStream out,
				Class<K> keyClass, Class<V> valueClass, CompressionCodec codec,
				Counters.Counter writesCounter) throws IOException {
			this.writtenRecordsCounter = writesCounter;
			this.checksumOut = new IFileOutputStream(out);
			this.rawOut = out;
			this.start = this.rawOut.getPos();

			if (codec != null) {
				this.compressor = CodecPool.getCompressor(codec);
				this.compressor.reset();
				this.compressedOut = codec.createOutputStream(checksumOut,
						compressor);
				this.out = new FSDataOutputStream(this.compressedOut, null);
				this.compressOutput = true;
			} else {
				this.out = new FSDataOutputStream(checksumOut, null);
			}

			this.keyClass = keyClass;
			this.valueClass = valueClass;
			SerializationFactory serializationFactory = new SerializationFactory(
					conf);
			this.keySerializer = serializationFactory.getSerializer(keyClass);
			this.keySerializer.open(buffer);
			this.valueSerializer = serializationFactory
					.getSerializer(valueClass);
			this.valueSerializer.open(buffer);
		}

		public void close() throws IOException {

			// Close the serializers
			keySerializer.close();
			valueSerializer.close();

			// Write EOF_MARKER for key/value length
			WritableUtils.writeVInt(out, EOF_MARKER);
			WritableUtils.writeVInt(out, EOF_MARKER);
			decompressedBytesWritten += 2 * WritableUtils
					.getVIntSize(EOF_MARKER);

			// Flush the stream
			out.flush();

			if (compressOutput) {
				// Flush
				compressedOut.finish();
				compressedOut.resetState();
			}

			// Close the underlying stream iff we own it...
			if (ownOutputStream) {
				out.close();
			} else {
				// Write the checksum
				checksumOut.finish();
			}

			compressedBytesWritten = rawOut.getPos() - start;

			if (compressOutput) {
				// Return back the compressor
				CodecPool.returnCompressor(compressor);
				compressor = null;
			}

			out = null;
			if (writtenRecordsCounter != null) {
				writtenRecordsCounter.increment(numRecordsWritten);
			}
		}

		public void append(K key, V value) throws IOException {
			if (key.getClass() != keyClass)
				throw new IOException("wrong key class: " + key.getClass()
						+ " is not " + keyClass);
			if (value.getClass() != valueClass)
				throw new IOException("wrong value class: " + value.getClass()
						+ " is not " + valueClass);

			// Append the 'key'
			keySerializer.serialize(key);
			int keyLength = buffer.getLength();
			if (keyLength < 0) {
				throw new IOException("Negative key-length not allowed: "
						+ keyLength + " for " + key);
			}

			// Append the 'value'
			valueSerializer.serialize(value);
			int valueLength = buffer.getLength() - keyLength;
			if (valueLength < 0) {
				throw new IOException("Negative value-length not allowed: "
						+ valueLength + " for " + value);
			}

			// Write the record out
			WritableUtils.writeVInt(out, keyLength); // key length
			WritableUtils.writeVInt(out, valueLength); // value length

			/********************************
			 * same key length and same value length, need optimize later
			 */

			// Log.info("key length : " + keyLength + " value length : " +
			// valueLength);
			out.write(buffer.getData(), 0, buffer.getLength()); // data

			// Reset
			buffer.reset();

			// Update bytes written
			decompressedBytesWritten += keyLength + valueLength
					+ WritableUtils.getVIntSize(keyLength)
					+ WritableUtils.getVIntSize(valueLength);
			++numRecordsWritten;
		}

		public void append(DataInputBuffer key, DataInputBuffer value)
				throws IOException {
			int keyLength = key.getLength() - key.getPosition();
			if (keyLength < 0) {
				throw new IOException("Negative key-length not allowed: "
						+ keyLength + " for " + key);
			}

			int valueLength = value.getLength() - value.getPosition();
			if (valueLength < 0) {
				throw new IOException("Negative value-length not allowed: "
						+ valueLength + " for " + value);
			}

			WritableUtils.writeVInt(out, keyLength);
			WritableUtils.writeVInt(out, valueLength);
			out.write(key.getData(), key.getPosition(), keyLength);
			out.write(value.getData(), value.getPosition(), valueLength);

			// Update bytes written
			decompressedBytesWritten += keyLength + valueLength
					+ WritableUtils.getVIntSize(keyLength)
					+ WritableUtils.getVIntSize(valueLength);
			++numRecordsWritten;
		}

		public long getRawLength() {
			return decompressedBytesWritten;
		}

		public long getCompressedLength() {
			return compressedBytesWritten;
		}
	}

	/**
	 * <code>IFile.SKVWriter</code> to write out intermediate map-outputs.
	 */
	public static class TrippleWriter<T1 extends Object, T2 extends Object, T3 extends Object> {
		FSDataOutputStream out;
		boolean ownOutputStream = false;
		long start = 0;
		FSDataOutputStream rawOut;

		CompressionOutputStream compressedOut;
		Compressor compressor;
		boolean compressOutput = false;

		long decompressedBytesWritten = 0;
		long compressedBytesWritten = 0;

		// Count records written to disk
		private long numRecordsWritten = 0;
		private final Counters.Counter writtenRecordsCounter;

		IFileOutputStream checksumOut;

		Class<T1> t1Class;
		Class<T2> t2Class;
		Class<T3> t3Class;
		Serializer<T1> t1Serializer;
		Serializer<T2> t2Serializer;
		Serializer<T3> t3Serializer;

		DataOutputBuffer buffer = new DataOutputBuffer();

		public TrippleWriter(Configuration conf, FileSystem fs, Path file,
				Class<T1> t1Class, Class<T2> t2Class, Class<T3> t3Class,
				CompressionCodec codec, Counters.Counter writesCounter)
				throws IOException {
			this(conf, fs.create(file), t1Class, t2Class, t3Class, codec,
					writesCounter);
			ownOutputStream = true;
		}

		public TrippleWriter(Configuration conf, FSDataOutputStream out,
				Class<T1> t1Class, Class<T2> t2Class, Class<T3> t3Class,
				CompressionCodec codec, Counters.Counter writesCounter)
				throws IOException {
			this.writtenRecordsCounter = writesCounter;
			this.checksumOut = new IFileOutputStream(out);
			this.rawOut = out;
			this.start = this.rawOut.getPos();

			if (codec != null) {
				this.compressor = CodecPool.getCompressor(codec);
				this.compressor.reset();
				this.compressedOut = codec.createOutputStream(checksumOut,
						compressor);
				this.out = new FSDataOutputStream(this.compressedOut, null);
				this.compressOutput = true;
			} else {
				this.out = new FSDataOutputStream(checksumOut, null);
			}

			this.t1Class = t1Class;
			this.t2Class = t2Class;
			this.t3Class = t3Class;
			SerializationFactory serializationFactory = new SerializationFactory(
					conf);
			this.t1Serializer = serializationFactory.getSerializer(t1Class);
			this.t1Serializer.open(buffer);
			this.t2Serializer = serializationFactory.getSerializer(t2Class);
			this.t2Serializer.open(buffer);
			this.t3Serializer = serializationFactory.getSerializer(t3Class);
			this.t3Serializer.open(buffer);
		}

		public long getPos() throws IOException {
			return out.getPos();
		}

		public void close() throws IOException {

			// Close the serializers
			t1Serializer.close();
			t2Serializer.close();
			t3Serializer.close();

			// Write EOF_MARKER for key/value length
			WritableUtils.writeVInt(out, EOF_MARKER);
			WritableUtils.writeVInt(out, EOF_MARKER);
			WritableUtils.writeVInt(out, EOF_MARKER);
			decompressedBytesWritten += 3 * WritableUtils
					.getVIntSize(EOF_MARKER);

			// Flush the stream
			out.flush();

			if (compressOutput) {
				// Flush
				compressedOut.finish();
				compressedOut.resetState();
			}

			// Close the underlying stream iff we own it...
			if (ownOutputStream) {
				out.close();
			} else {
				// Write the checksum
				checksumOut.finish();
			}

			compressedBytesWritten = rawOut.getPos() - start;

			if (compressOutput) {
				// Return back the compressor
				CodecPool.returnCompressor(compressor);
				compressor = null;
			}

			out = null;
			if (writtenRecordsCounter != null) {
				writtenRecordsCounter.increment(numRecordsWritten);
			}
		}

		public void append(T1 t1, T2 t2, T3 t3) throws IOException {
			if (t1.getClass() != t1Class)
				throw new IOException("wrong t1 class: " + t1.getClass()
						+ " is not " + t1Class);
			if (t2.getClass() != t2Class)
				throw new IOException("wrong t2 class: " + t2.getClass()
						+ " is not " + t2Class);
			if (t3.getClass() != t3Class)
				throw new IOException("wrong t3 class: " + t3.getClass()
						+ " is not " + t3Class);

			// Append the 'skey'
			t1Serializer.serialize(t1);
			int t1Length = buffer.getLength();
			if (t1Length < 0) {
				throw new IOException("Negative key-length not allowed: "
						+ t1Length + " for " + t1);
			}

			// Append the 'key'
			t2Serializer.serialize(t2);
			int t2Length = buffer.getLength() - t1Length;
			if (t2Length < 0) {
				throw new IOException("Negative t2-length not allowed: "
						+ t2Length + " for " + t2);
			}

			// Append the 'value'
			t3Serializer.serialize(t3);
			int t3Length = buffer.getLength() - t2Length - t1Length;
			if (t3Length < 0) {
				throw new IOException("Negative t3-length not allowed: "
						+ t3Length + " for " + t3);
			}

			// Write the record out
			WritableUtils.writeVInt(out, t1Length); // key length
			WritableUtils.writeVInt(out, t2Length); // key length
			WritableUtils.writeVInt(out, t3Length); // value length

			/********************************
			 * same key length and same value length, need optimize later
			 */

			// Log.info("key length : " + keyLength + " value length : " +
			// valueLength);
			out.write(buffer.getData(), 0, buffer.getLength()); // data

			// Reset
			buffer.reset();

			// Update bytes written
			decompressedBytesWritten += t1Length + t2Length + t3Length
					+ WritableUtils.getVIntSize(t1Length)
					+ WritableUtils.getVIntSize(t2Length)
					+ WritableUtils.getVIntSize(t3Length);
			++numRecordsWritten;
		}

		public void append(DataInputBuffer t1, DataInputBuffer t2,
				DataInputBuffer t3) throws IOException {
			int t1Length = t1.getLength() - t1.getPosition();
			if (t1Length < 0) {
				throw new IOException("Negative t1-length not allowed: "
						+ t1Length + " for ");
			}

			int t2Length = t2.getLength() - t2.getPosition();
			if (t2Length < 0) {
				throw new IOException("Negative t2-length not allowed: "
						+ t2Length + " for " + t2);
			}

			int t3Length = t3.getLength() - t3.getPosition();
			if (t3Length < 0) {
				throw new IOException("Negative t3-length not allowed: "
						+ t2Length + " for " + t3);
			}

			WritableUtils.writeVInt(out, t1Length);
			WritableUtils.writeVInt(out, t2Length);
			WritableUtils.writeVInt(out, t3Length);
			out.write(t1.getData(), t1.getPosition(), t1Length);
			out.write(t2.getData(), t2.getPosition(), t2Length);
			out.write(t3.getData(), t3.getPosition(), t3Length);

			// Update bytes written
			decompressedBytesWritten += t1Length + t2Length + t3Length
					+ WritableUtils.getVIntSize(t1Length)
					+ WritableUtils.getVIntSize(t2Length)
					+ WritableUtils.getVIntSize(t3Length);
			++numRecordsWritten;
		}

		public long getRawLength() {
			return decompressedBytesWritten;
		}

		public long getCompressedLength() {
			return compressedBytesWritten;
		}
	}

	/**
	 * <code>IFile.Writer</code> to write out intermediate map-outputs.
	 */
	public static class FixedLengthWriter<K extends Object, V extends Object> {
		FSDataOutputStream out;
		boolean ownOutputStream = false;
		long start = 0;
		FSDataOutputStream rawOut;
		boolean first = true; // record the k,v length
		int keyLength = 0;
		int valueLength;

		CompressionOutputStream compressedOut;
		Compressor compressor;
		boolean compressOutput = false;

		long decompressedBytesWritten = 0;
		long compressedBytesWritten = 0;

		// Count records written to disk
		private long numRecordsWritten = 0;
		private final Counters.Counter writtenRecordsCounter;

		IFileOutputStream checksumOut;

		Class<K> keyClass;
		Class<V> valueClass;
		Serializer<K> keySerializer;
		Serializer<V> valueSerializer;

		DataOutputBuffer buffer = new DataOutputBuffer();

		public FixedLengthWriter(Configuration conf, FileSystem fs, Path file,
				Class<K> keyClass, Class<V> valueClass, CompressionCodec codec,
				Counters.Counter writesCounter) throws IOException {
			this(conf, fs.create(file), keyClass, valueClass, codec,
					writesCounter);
			ownOutputStream = true;
		}

		public FixedLengthWriter(Configuration conf, FSDataOutputStream out,
				Class<K> keyClass, Class<V> valueClass, CompressionCodec codec,
				Counters.Counter writesCounter) throws IOException {
			this.writtenRecordsCounter = writesCounter;
			this.checksumOut = new IFileOutputStream(out);
			this.rawOut = out;
			this.start = this.rawOut.getPos();

			if (codec != null) {
				this.compressor = CodecPool.getCompressor(codec);
				this.compressor.reset();
				this.compressedOut = codec.createOutputStream(checksumOut,
						compressor);
				this.out = new FSDataOutputStream(this.compressedOut, null);
				this.compressOutput = true;
			} else {
				this.out = new FSDataOutputStream(checksumOut, null);
			}

			this.keyClass = keyClass;
			this.valueClass = valueClass;
			SerializationFactory serializationFactory = new SerializationFactory(
					conf);
			this.keySerializer = serializationFactory.getSerializer(keyClass);
			this.keySerializer.open(buffer);
			this.valueSerializer = serializationFactory
					.getSerializer(valueClass);
			this.valueSerializer.open(buffer);
		}

		public void close() throws IOException {

			// Close the serializers
			keySerializer.close();
			valueSerializer.close();

			// Write EOF_MARKER for key/value length
			WritableUtils.writeVInt(out, EOF_MARKER);
			WritableUtils.writeVInt(out, EOF_MARKER);
			decompressedBytesWritten += 2 * WritableUtils
					.getVIntSize(EOF_MARKER);

			// Flush the stream
			out.flush();

			if (compressOutput) {
				// Flush
				compressedOut.finish();
				compressedOut.resetState();
			}

			// Close the underlying stream iff we own it...
			if (ownOutputStream) {
				out.close();
			} else {
				// Write the checksum
				checksumOut.finish();
			}

			compressedBytesWritten = rawOut.getPos() - start;

			if (compressOutput) {
				// Return back the compressor
				CodecPool.returnCompressor(compressor);
				compressor = null;
			}

			out = null;
			if (writtenRecordsCounter != null) {
				writtenRecordsCounter.increment(numRecordsWritten);
			}
		}

		public void append(K key, V value) throws IOException {
			if (key.getClass() != keyClass)
				throw new IOException("wrong key class: " + key.getClass()
						+ " is not " + keyClass);
			if (value.getClass() != valueClass)
				throw new IOException("wrong value class: " + value.getClass()
						+ " is not " + valueClass);

			if (first) {
				// Append the 'key'
				keySerializer.serialize(key);

				keyLength = buffer.getLength();
				if (keyLength < 0) {
					throw new IOException("Negative key-length not allowed: "
							+ keyLength + " for " + key);
				}

				// Append the 'value'
				valueSerializer.serialize(value);
				valueLength = buffer.getLength() - keyLength;
				if (valueLength < 0) {
					throw new IOException("Negative value-length not allowed: "
							+ valueLength + " for " + value);
				}

				// Write the record out
				WritableUtils.writeVInt(out, keyLength); // key length
				WritableUtils.writeVInt(out, valueLength); // value length

				// Update bytes written
				decompressedBytesWritten += WritableUtils
						.getVIntSize(keyLength)
						+ WritableUtils.getVIntSize(valueLength);

				first = false;
			} else {
				keySerializer.serialize(key);
				valueSerializer.serialize(value);
			}

			/********************************
			 * same key length and same value length, need optimize later
			 */

			// Log.info("key length : " + keyLength + " value length : " +
			// valueLength);
			out.write(buffer.getData(), 0, buffer.getLength()); // data

			// Reset
			buffer.reset();

			// Update bytes written
			decompressedBytesWritten += keyLength + valueLength;
			++numRecordsWritten;
		}

		public void append(DataInputBuffer key, DataInputBuffer value)
				throws IOException {
			int keyLength = key.getLength() - key.getPosition();
			if (keyLength < 0) {
				throw new IOException("Negative key-length not allowed: "
						+ keyLength + " for " + key);
			}

			int valueLength = value.getLength() - value.getPosition();
			if (valueLength < 0) {
				throw new IOException("Negative value-length not allowed: "
						+ valueLength + " for " + value);
			}

			WritableUtils.writeVInt(out, keyLength);
			WritableUtils.writeVInt(out, valueLength);
			out.write(key.getData(), key.getPosition(), keyLength);
			out.write(value.getData(), value.getPosition(), valueLength);

			// Update bytes written
			decompressedBytesWritten += keyLength + valueLength
					+ WritableUtils.getVIntSize(keyLength)
					+ WritableUtils.getVIntSize(valueLength);
			++numRecordsWritten;
		}

		public long getRawLength() {
			return decompressedBytesWritten;
		}

		public long getCompressedLength() {
			return compressedBytesWritten;
		}
	}

	/**
	 * <code>IFile.Reader</code> to read intermediate map-outputs.
	 */
	public static class Reader<K extends Object, V extends Object> {
		private static final int DEFAULT_BUFFER_SIZE = 128 * 1024;
		private static final int MAX_VINT_SIZE = 9;

		// Count records read from disk
		private long numRecordsRead = 0;
		private final Counters.Counter readRecordsCounter;

		final InputStream in; // Possibly decompressed stream that we read
		Decompressor decompressor;
		long bytesRead = 0;
		final long fileLength;
		boolean eof = false;
		final IFileInputStream checksumIn;

		byte[] buffer = null;
		int bufferSize = DEFAULT_BUFFER_SIZE;
		DataInputBuffer dataIn = new DataInputBuffer();

		int recNo = 1;

		/**
		 * Construct an IFile Reader.
		 * 
		 * @param conf
		 *            Configuration File
		 * @param fs
		 *            FileSystem
		 * @param file
		 *            Path of the file to be opened. This file should have
		 *            checksum bytes for the data at the end of the file.
		 * @param codec
		 *            codec
		 * @param readsCounter
		 *            Counter for records read from disk
		 * @throws IOException
		 */
		public Reader(Configuration conf, FileSystem fs, Path file,
				CompressionCodec codec, Counters.Counter readsCounter)
				throws IOException {
			this(conf, fs.open(file), fs.getFileStatus(file).getLen(), codec,
					readsCounter);
		}

		/**
		 * Construct an IFile Reader.
		 * 
		 * @param conf
		 *            Configuration File
		 * @param in
		 *            The input stream
		 * @param length
		 *            Length of the data in the stream, including the checksum
		 *            bytes.
		 * @param codec
		 *            codec
		 * @param readsCounter
		 *            Counter for records read from disk
		 * @throws IOException
		 */
		public Reader(Configuration conf, FSDataInputStream in, long length,
				CompressionCodec codec, Counters.Counter readsCounter)
				throws IOException {
			readRecordsCounter = readsCounter;
			checksumIn = new IFileInputStream(in, length);
			if (codec != null) {
				decompressor = CodecPool.getDecompressor(codec);
				this.in = codec.createInputStream(checksumIn, decompressor);
			} else {
				this.in = checksumIn;
			}
			this.fileLength = length;

			if (conf != null) {
				bufferSize = conf.getInt("io.file.buffer.size",
						DEFAULT_BUFFER_SIZE);
			}
		}

		public long getLength() {
			return fileLength - checksumIn.getSize();
		}

		public long getPosition() throws IOException {
			return checksumIn.getPosition();
		}

		/**
		 * Read upto len bytes into buf starting at offset off.
		 * 
		 * @param buf
		 *            buffer
		 * @param off
		 *            offset
		 * @param len
		 *            length of buffer
		 * @return the no. of bytes read
		 * @throws IOException
		 */
		private int readData(byte[] buf, int off, int len) throws IOException {
			int bytesRead = 0;
			while (bytesRead < len) {
				int n = in.read(buf, off + bytesRead, len - bytesRead);
				if (n < 0) {
					return bytesRead;
				}
				bytesRead += n;
			}
			return len;
		}

		void readNextBlock(int minSize) throws IOException {
			if (buffer == null) {
				buffer = new byte[bufferSize];
				dataIn.reset(buffer, 0, 0);
			}
			buffer = rejigData(buffer,
					(bufferSize < minSize) ? new byte[minSize << 1] : buffer);
			bufferSize = buffer.length;
		}

		private byte[] rejigData(byte[] source, byte[] destination)
				throws IOException {
			// Copy remaining data into the destination array
			int bytesRemaining = dataIn.getLength() - dataIn.getPosition();
			if (bytesRemaining > 0) {
				System.arraycopy(source, dataIn.getPosition(), destination, 0,
						bytesRemaining);
			}

			// Read as much data as will fit from the underlying stream
			int n = readData(destination, bytesRemaining,
					(destination.length - bytesRemaining));
			dataIn.reset(destination, 0, (bytesRemaining + n));

			return destination;
		}

		public boolean next(DataInputBuffer key, DataInputBuffer value)
				throws IOException {
			// Sanity check
			if (eof) {
				throw new EOFException("Completed reading " + bytesRead);
			}

			// Check if we have enough data to read lengths
			if ((dataIn.getLength() - dataIn.getPosition()) < 2 * MAX_VINT_SIZE) {
				readNextBlock(2 * MAX_VINT_SIZE);
			}

			// Read key and value lengths
			int oldPos = dataIn.getPosition();
			int keyLength = WritableUtils.readVInt(dataIn);
			int valueLength = WritableUtils.readVInt(dataIn);
			int pos = dataIn.getPosition();
			bytesRead += pos - oldPos;

			// Check for EOF
			if (keyLength == EOF_MARKER && valueLength == EOF_MARKER) {
				eof = true;
				return false;
			}

			// Sanity check
			if (keyLength < 0) {
				throw new IOException("Rec# " + recNo
						+ ": Negative key-length: " + keyLength);
			}
			if (valueLength < 0) {
				throw new IOException("Rec# " + recNo
						+ ": Negative value-length: " + valueLength);
			}

			final int recordLength = keyLength + valueLength;

			// Check if we have the raw key/value in the buffer
			if ((dataIn.getLength() - pos) < recordLength) {
				readNextBlock(recordLength);

				// Sanity check
				if ((dataIn.getLength() - dataIn.getPosition()) < recordLength) {
					throw new EOFException("Rec# " + recNo
							+ ": Could read the next " + " record");
				}
			}

			// Setup the key and value
			pos = dataIn.getPosition();
			byte[] data = dataIn.getData();
			key.reset(data, pos, keyLength);
			value.reset(data, (pos + keyLength), valueLength);

			// Position for the next record
			long skipped = dataIn.skip(recordLength);
			if (skipped != recordLength) {
				throw new IOException("Rec# " + recNo
						+ ": Failed to skip past record " + "of length: "
						+ recordLength);
			}

			// Record the bytes read
			bytesRead += recordLength;

			++recNo;
			++numRecordsRead;

			return true;
		}

		public void close() throws IOException {
			// Return the decompressor
			if (decompressor != null) {
				decompressor.reset();
				CodecPool.returnDecompressor(decompressor);
				decompressor = null;
			}

			// Close the underlying stream
			in.close();

			// Release the buffer
			dataIn = null;
			buffer = null;
			if (readRecordsCounter != null) {
				readRecordsCounter.increment(numRecordsRead);
			}
		}
	}

	/**
	 * <code>IFile.SKVReader</code> to read intermediate map-outputs.
	 */
	public static class TrippleReader<T1 extends Object, T2 extends Object, T3 extends Object> {
		private static final int DEFAULT_BUFFER_SIZE = 128 * 1024;
		private static final int MAX_VINT_SIZE = 9;

		// Count records read from disk
		private long numRecordsRead = 0;
		private final Counters.Counter readRecordsCounter;

		final InputStream in; // Possibly decompressed stream that we read
		Decompressor decompressor;
		long bytesRead = 0;
		final long fileLength;
		boolean eof = false;
		final IFileInputStream checksumIn;

		byte[] buffer = null;
		int bufferSize = DEFAULT_BUFFER_SIZE;
		DataInputBuffer dataIn = new DataInputBuffer();

		int recNo = 1;

		/**
		 * Construct an IFile Reader.
		 * 
		 * @param conf
		 *            Configuration File
		 * @param fs
		 *            FileSystem
		 * @param file
		 *            Path of the file to be opened. This file should have
		 *            checksum bytes for the data at the end of the file.
		 * @param codec
		 *            codec
		 * @param readsCounter
		 *            Counter for records read from disk
		 * @throws IOException
		 */
		public TrippleReader(Configuration conf, FileSystem fs, Path file,
				CompressionCodec codec, Counters.Counter readsCounter)
				throws IOException {
			this(conf, fs.open(file), fs.getFileStatus(file).getLen(), codec,
					readsCounter);
		}

		/**
		 * Construct an IFile Reader.
		 * 
		 * @param conf
		 *            Configuration File
		 * @param in
		 *            The input stream
		 * @param length
		 *            Length of the data in the stream, including the checksum
		 *            bytes.
		 * @param codec
		 *            codec
		 * @param readsCounter
		 *            Counter for records read from disk
		 * @throws IOException
		 */
		public TrippleReader(Configuration conf, FSDataInputStream in,
				long length, CompressionCodec codec,
				Counters.Counter readsCounter) throws IOException {
			readRecordsCounter = readsCounter;
			checksumIn = new IFileInputStream(in, length);
			if (codec != null) {
				decompressor = CodecPool.getDecompressor(codec);
				this.in = codec.createInputStream(checksumIn, decompressor);
			} else {
				this.in = checksumIn;
			}
			this.fileLength = length;

			if (conf != null) {
				bufferSize = conf.getInt("io.file.buffer.size",
						DEFAULT_BUFFER_SIZE);
			}
		}

		public long getLength() {
			return fileLength - checksumIn.getSize();
		}

		public long getPosition() throws IOException {
			return checksumIn.getPosition();
		}

		/**
		 * Read upto len bytes into buf starting at offset off.
		 * 
		 * @param buf
		 *            buffer
		 * @param off
		 *            offset
		 * @param len
		 *            length of buffer
		 * @return the no. of bytes read
		 * @throws IOException
		 */
		private int readData(byte[] buf, int off, int len) throws IOException {
			int bytesRead = 0;
			while (bytesRead < len) {
				int n = in.read(buf, off + bytesRead, len - bytesRead);
				if (n < 0) {
					return bytesRead;
				}
				bytesRead += n;
			}
			return len;
		}

		void readNextBlock(int minSize) throws IOException {
			if (buffer == null) {
				buffer = new byte[bufferSize];
				dataIn.reset(buffer, 0, 0);
			}
			buffer = rejigData(buffer,
					(bufferSize < minSize) ? new byte[minSize << 1] : buffer);
			bufferSize = buffer.length;
		}

		private byte[] rejigData(byte[] source, byte[] destination)
				throws IOException {
			// Copy remaining data into the destination array
			int bytesRemaining = dataIn.getLength() - dataIn.getPosition();
			if (bytesRemaining > 0) {
				System.arraycopy(source, dataIn.getPosition(), destination, 0,
						bytesRemaining);
			}

			// Read as much data as will fit from the underlying stream
			int n = readData(destination, bytesRemaining,
					(destination.length - bytesRemaining));
			dataIn.reset(destination, 0, (bytesRemaining + n));

			return destination;
		}

		public boolean next(DataInputBuffer t1, DataInputBuffer t2,
				DataInputBuffer t3) throws IOException {
			// Sanity check
			if (eof) {
				throw new EOFException("Completed reading " + bytesRead);
			}

			// Check if we have enough data to read lengths
			if ((dataIn.getLength() - dataIn.getPosition()) < 3 * MAX_VINT_SIZE) {
				readNextBlock(3 * MAX_VINT_SIZE);
			}

			// Read key and value lengths
			int oldPos = dataIn.getPosition();
			int t1Length = WritableUtils.readVInt(dataIn);
			int t2Length = WritableUtils.readVInt(dataIn);
			int t3Length = WritableUtils.readVInt(dataIn);
			// Log.info(oldPos+" "+t1Length+" "+t2Length+" "+t3Length);
			int pos = dataIn.getPosition();
			bytesRead += pos - oldPos;

			// Check for EOF
			if (t1Length == EOF_MARKER && t2Length == EOF_MARKER
					&& t3Length == EOF_MARKER) {
				eof = true;
				// Log.info("returning false");;
				return false;
			}

			// Sanity check
			if (t1Length < 0) {
				throw new IOException("Rec# " + recNo
						+ ": Negative t1-length: " + t1Length);
			}
			if (t2Length < 0) {
				throw new IOException("Rec# " + recNo
						+ ": Negative t2-length: " + t2Length);
			}
			if (t3Length < 0) {
				throw new IOException("Rec# " + recNo
						+ ": Negative t3-length: " + t3Length);
			}

			final int recordLength = t1Length + t2Length + t3Length;

			// Check if we have the raw key/value in the buffer
			if ((dataIn.getLength() - pos) < recordLength) {
				readNextBlock(recordLength);

				// Sanity check
				if ((dataIn.getLength() - dataIn.getPosition()) < recordLength) {
					throw new EOFException("Rec# " + recNo
							+ ": Could read the next " + " record");
				}
			}

			// Setup the key and value
			pos = dataIn.getPosition();
			byte[] data = dataIn.getData();
			t1.reset(data, pos, t1Length);
			t2.reset(data, (pos + t1Length), t2Length);
			t3.reset(data, (pos + t1Length + t2Length), t3Length);

			// Position for the next record
			long skipped = dataIn.skip(recordLength);
			if (skipped != recordLength) {
				throw new IOException("Rec# " + recNo
						+ ": Failed to skip past record " + "of length: "
						+ recordLength);
			}

			// Record the bytes read
			bytesRead += recordLength;

			++recNo;
			++numRecordsRead;

			return true;
		}

		public void close() throws IOException {
			// Return the decompressor
			if (decompressor != null) {
				decompressor.reset();
				CodecPool.returnDecompressor(decompressor);
				decompressor = null;
			}

			// Close the underlying stream
			in.close();

			// Release the buffer
			dataIn = null;
			buffer = null;
			if (readRecordsCounter != null) {
				readRecordsCounter.increment(numRecordsRead);
			}
		}
	}

	/**
	 * <code>IFile.RandomAccessReader</code> to randomly read intermediate
	 * map-outputs.
	 */
	public static class RandomAccessReader<K extends Object, T2 extends Object, T3 extends Object> {
		private static final int DEFAULT_BUFFER_SIZE = 128 * 1024;
		private static final int MAX_VINT_SIZE = 9;
		private static final int INDEX_ENTRY_SIZE = 8;

		// Count records read from disk
		private long numRecordsRead = 0;
		private final Counters.Counter readRecordsCounter;

		RandomAccessFile dataIns;
		FSDataInputStream indexIns;
		TreeMap<Integer, Integer> indexCache;

		long bytesRead = 0;
		final long fileLength;
		boolean eof = false;
		boolean indexend = false;

		byte[] buffer = null;
		byte[] indexbuffer = null;
		int bufferSize = DEFAULT_BUFFER_SIZE;
		int indexbufferSize = DEFAULT_BUFFER_SIZE;
		DataInputBuffer dataIn = new DataInputBuffer();
		DataInputBuffer indexIn = new DataInputBuffer();

		Class<K> t1Class;

		int recNo = 1;

		/**
		 * Construct an IFile Reader.
		 * 
		 * @param conf
		 *            Configuration File
		 * @param in
		 *            The input stream
		 * @param length
		 *            Length of the data in the stream, including the checksum
		 *            bytes.
		 * @param codec
		 *            codec
		 * @param readsCounter
		 *            Counter for records read from disk
		 * @throws IOException
		 */
		public RandomAccessReader(Configuration conf, Path datafile,
				Path indexfile, Class<K> keyclass, CompressionCodec codec,
				Counters.Counter readsCounter) throws IOException {
			readRecordsCounter = readsCounter;
			FileSystem fs = FileSystem.getLocal(conf);
			dataIns = new RandomAccessFile(datafile.toString(), "r");
			indexIns = fs.open(indexfile);
			this.fileLength = fs.getFileStatus(datafile).getLen();

			if (conf != null) {
				bufferSize = conf.getInt("io.file.buffer.size",
						DEFAULT_BUFFER_SIZE);
			}

			t1Class = keyclass;
			buffer = new byte[bufferSize];
			dataIn.reset(buffer, 0, 0);
			indexbuffer = new byte[indexbufferSize];
			indexIn.reset(indexbuffer, 0, 0);

			// load the indices
			indexCache = new TreeMap<Integer, Integer>();

			while (true) {
				if (indexIn.getLength() - indexIn.getPosition() < INDEX_ENTRY_SIZE) {
					if (indexend)
						break;

					readNextBlock(INDEX_ENTRY_SIZE);
				}

				int hashcode = indexIn.readInt();
				int offset = indexIn.readInt();

				indexCache.put(hashcode, offset);
			}
			indexIn.close();
			indexbuffer = null;
		}

		public long getLength() {
			return fileLength;
		}

		/**
		 * Read upto len bytes into buf starting at offset off.
		 * 
		 * @param buf
		 *            buffer
		 * @param off
		 *            offset
		 * @param len
		 *            length of buffer
		 * @return the no. of bytes read
		 * @throws IOException
		 */
		private int readIndexData(byte[] buf, int off, int len)
				throws IOException {
			int bytesRead = 0;
			while (bytesRead < len) {
				int n = indexIns.read(buf, off + bytesRead, len - bytesRead);
				if (n < 0) {
					indexend = true;
					return bytesRead;
				}
				bytesRead += n;
			}
			return len;
		}

		private int readData(byte[] buf, int off, int len) throws IOException {
			int bytesRead = 0;
			while (bytesRead < len) {
				int n = dataIns.read(buf, off + bytesRead, len - bytesRead);
				if (n < 0) {
					return bytesRead;
				}
				bytesRead += n;
			}
			return len;
		}

		void readNextBlock(int minSize) throws IOException {
			buffer = rejigData(buffer,
					(bufferSize < minSize) ? new byte[minSize << 1] : buffer);
			bufferSize = buffer.length;
		}

		void readIndexNextBlock(int minSize) throws IOException {
			indexbuffer = rejigIndexData(indexbuffer,
					(indexbufferSize < minSize) ? new byte[minSize << 1]
							: indexbuffer);
			indexbufferSize = indexbuffer.length;
		}

		private byte[] rejigData(byte[] source, byte[] destination)
				throws IOException {
			// Copy remaining data into the destination array
			int bytesRemaining = dataIn.getLength() - dataIn.getPosition();
			if (bytesRemaining > 0) {
				System.arraycopy(source, dataIn.getPosition(), destination, 0,
						bytesRemaining);
			}

			// Read as much data as will fit from the underlying stream
			int n = readData(destination, bytesRemaining,
					(destination.length - bytesRemaining));
			dataIn.reset(destination, 0, (bytesRemaining + n));

			return destination;
		}

		private byte[] rejigIndexData(byte[] source, byte[] destination)
				throws IOException {
			// Copy remaining data into the destination array
			int bytesRemaining = indexIn.getLength() - indexIn.getPosition();
			if (bytesRemaining > 0) {
				System.arraycopy(source, indexIn.getPosition(), destination, 0,
						bytesRemaining);
			}

			// Read as much data as will fit from the underlying stream
			int n = readIndexData(destination, bytesRemaining,
					(destination.length - bytesRemaining));
			indexIn.reset(destination, 0, (bytesRemaining + n));

			return destination;
		}

		public boolean seekKey(K key) throws IOException {
			if (indexCache.get(key.hashCode()) == null)
				return false;

			dataIns.seek(indexCache.get(key.hashCode()));
			int n = readData(buffer, 0, bufferSize);
			dataIn.reset(buffer, 0, n);
			return true;
		}

		// return the correct key hashcode, hash conflict-avoidence
		public boolean next(DataInputBuffer t1, DataInputBuffer t2,
				DataInputBuffer t3) throws IOException {
			// Sanity check
			if (eof) {
				throw new EOFException("Completed reading " + bytesRead);
			}

			// Check if we have enough data to read lengths
			if ((dataIn.getLength() - dataIn.getPosition()) < 3 * MAX_VINT_SIZE) {
				readNextBlock(3 * MAX_VINT_SIZE);
			}

			// Read key and value lengths
			int oldPos = dataIn.getPosition();
			int t1Length = WritableUtils.readVInt(dataIn);
			int t2Length = WritableUtils.readVInt(dataIn);
			int t3Length = WritableUtils.readVInt(dataIn);
			int pos = dataIn.getPosition();
			bytesRead += pos - oldPos;

			// Check for EOF
			if (t1Length == EOF_MARKER && t2Length == EOF_MARKER
					&& t3Length == EOF_MARKER) {
				eof = true;
				return false;
			}

			// Sanity check
			if (t1Length < 0) {
				throw new IOException("Rec# " + recNo
						+ ": Negative t1-length: " + t1Length);
			}
			if (t2Length < 0) {
				throw new IOException("Rec# " + recNo
						+ ": Negative t2-length: " + t2Length);
			}
			if (t3Length < 0) {
				throw new IOException("Rec# " + recNo
						+ ": Negative t3-length: " + t3Length);
			}

			final int recordLength = t1Length + t2Length + t3Length;

			// Check if we have the raw key/value in the buffer
			if ((dataIn.getLength() - pos) < recordLength) {
				readNextBlock(recordLength);

				// Sanity check
				if ((dataIn.getLength() - dataIn.getPosition()) < recordLength) {
					throw new EOFException("Rec# " + recNo
							+ ": Could read the next " + " record");
				}
			}

			// Setup the key and value
			pos = dataIn.getPosition();
			byte[] data = dataIn.getData();
			t1.reset(data, pos, t1Length);
			t2.reset(data, (pos + t1Length), t2Length);
			t3.reset(data, (pos + t1Length + t2Length), t3Length);

			// Position for the next record
			long skipped = dataIn.skip(recordLength);
			if (skipped != recordLength) {
				throw new IOException("Rec# " + recNo
						+ ": Failed to skip past record " + "of length: "
						+ recordLength);
			}

			// Record the bytes read
			bytesRead += recordLength;

			++recNo;
			++numRecordsRead;

			return true;
		}

		public void close() throws IOException {
			// Close the underlying stream
			dataIn.close();

			// Release the buffer
			dataIn = null;
			buffer = null;
			if (readRecordsCounter != null) {
				readRecordsCounter.increment(numRecordsRead);
			}

		}
	}

	/**
	 * <code>IFile.Reader</code> to read intermediate map-outputs.
	 */
	public static class FixedLengthReader<K extends Object, V extends Object> {
		private static final int DEFAULT_BUFFER_SIZE = 128 * 1024;
		private static final int MAX_VINT_SIZE = 9;

		// Count records read from disk
		private long numRecordsRead = 0;
		private final Counters.Counter readRecordsCounter;

		final InputStream in; // Possibly decompressed stream that we read
		Decompressor decompressor;
		long bytesRead = 0;
		final long fileLength;
		boolean eof = false;
		final IFileInputStream checksumIn;

		byte[] buffer = null;
		int bufferSize = DEFAULT_BUFFER_SIZE;
		DataInputBuffer dataIn = new DataInputBuffer();

		int recNo = 1;

		boolean first = true;
		int keyLength;
		int valueLength;

		/**
		 * Construct an IFile Reader.
		 * 
		 * @param conf
		 *            Configuration File
		 * @param fs
		 *            FileSystem
		 * @param file
		 *            Path of the file to be opened. This file should have
		 *            checksum bytes for the data at the end of the file.
		 * @param codec
		 *            codec
		 * @param readsCounter
		 *            Counter for records read from disk
		 * @throws IOException
		 */
		public FixedLengthReader(Configuration conf, FileSystem fs, Path file,
				CompressionCodec codec, Counters.Counter readsCounter)
				throws IOException {
			this(conf, fs.open(file), fs.getFileStatus(file).getLen(), codec,
					readsCounter);
		}

		/**
		 * Construct an IFile Reader.
		 * 
		 * @param conf
		 *            Configuration File
		 * @param in
		 *            The input stream
		 * @param length
		 *            Length of the data in the stream, including the checksum
		 *            bytes.
		 * @param codec
		 *            codec
		 * @param readsCounter
		 *            Counter for records read from disk
		 * @throws IOException
		 */
		public FixedLengthReader(Configuration conf, FSDataInputStream in,
				long length, CompressionCodec codec,
				Counters.Counter readsCounter) throws IOException {
			readRecordsCounter = readsCounter;
			checksumIn = new IFileInputStream(in, length);
			if (codec != null) {
				decompressor = CodecPool.getDecompressor(codec);
				this.in = codec.createInputStream(checksumIn, decompressor);
			} else {
				this.in = checksumIn;
			}
			this.fileLength = length;

			if (conf != null) {
				bufferSize = conf.getInt("io.file.buffer.size",
						DEFAULT_BUFFER_SIZE);
			}
		}

		public long getLength() {
			return fileLength - checksumIn.getSize();
		}

		public long getPosition() throws IOException {
			return checksumIn.getPosition();
		}

		/**
		 * Read upto len bytes into buf starting at offset off.
		 * 
		 * @param buf
		 *            buffer
		 * @param off
		 *            offset
		 * @param len
		 *            length of buffer
		 * @return the no. of bytes read
		 * @throws IOException
		 */
		private int readData(byte[] buf, int off, int len) throws IOException {
			int bytesRead = 0;
			while (bytesRead < len) {
				int n = in.read(buf, off + bytesRead, len - bytesRead);
				if (n < 0) {
					return bytesRead;
				}
				bytesRead += n;
			}
			return len;
		}

		void readNextBlock(int minSize) throws IOException {
			if (buffer == null) {
				buffer = new byte[bufferSize];
				dataIn.reset(buffer, 0, 0);
			}
			buffer = rejigData(buffer,
					(bufferSize < minSize) ? new byte[minSize << 1] : buffer);
			bufferSize = buffer.length;
		}

		private byte[] rejigData(byte[] source, byte[] destination)
				throws IOException {
			// Copy remaining data into the destination array
			int bytesRemaining = dataIn.getLength() - dataIn.getPosition();
			if (bytesRemaining > 0) {
				System.arraycopy(source, dataIn.getPosition(), destination, 0,
						bytesRemaining);
			}

			// Read as much data as will fit from the underlying stream
			int n = readData(destination, bytesRemaining,
					(destination.length - bytesRemaining));
			dataIn.reset(destination, 0, (bytesRemaining + n));

			return destination;
		}

		public boolean next(DataInputBuffer key, DataInputBuffer value)
				throws IOException {
			// Sanity check
			if (eof) {
				throw new EOFException("Completed reading " + bytesRead);
			}

			// Check if we have enough data to read lengths
			if ((dataIn.getLength() - dataIn.getPosition()) < 2 * MAX_VINT_SIZE) {
				readNextBlock(2 * MAX_VINT_SIZE);
			}

			// Read key and value lengths
			int oldPos = dataIn.getPosition();
			if (first) {
				keyLength = WritableUtils.readVInt(dataIn);
				valueLength = WritableUtils.readVInt(dataIn);

			}
			int pos = dataIn.getPosition();
			bytesRead += pos - oldPos;

			first = false;

			// Check for EOF
			if (keyLength == EOF_MARKER && valueLength == EOF_MARKER) {
				eof = true;
				return false;
			}

			// Sanity check
			if (keyLength < 0) {
				throw new IOException("Rec# " + recNo
						+ ": Negative key-length: " + keyLength);
			}
			if (valueLength < 0) {
				throw new IOException("Rec# " + recNo
						+ ": Negative value-length: " + valueLength);
			}

			final int recordLength = keyLength + valueLength;

			// Check if we have the raw key/value in the buffer
			if ((dataIn.getLength() - pos) < recordLength) {
				readNextBlock(recordLength);

				// Sanity check
				if ((dataIn.getLength() - dataIn.getPosition()) < recordLength) {
					throw new EOFException("Rec# " + recNo
							+ ": Could read the next " + " record");
				}
			}

			// Setup the key and value
			pos = dataIn.getPosition();
			byte[] data = dataIn.getData();
			key.reset(data, pos, keyLength);
			value.reset(data, (pos + keyLength), valueLength);

			// Position for the next record
			long skipped = dataIn.skip(recordLength);
			if (skipped != recordLength) {
				throw new IOException("Rec# " + recNo
						+ ": Failed to skip past record " + "of length: "
						+ recordLength);
			}

			// Record the bytes read
			bytesRead += recordLength;

			++recNo;
			++numRecordsRead;

			return true;
		}

		public void close() throws IOException {
			// Return the decompressor
			if (decompressor != null) {
				decompressor.reset();
				CodecPool.returnDecompressor(decompressor);
				decompressor = null;
			}

			// Close the underlying stream
			in.close();

			// Release the buffer
			dataIn = null;
			buffer = null;
			if (readRecordsCounter != null) {
				readRecordsCounter.increment(numRecordsRead);
			}
		}
	}

	/**
	 * <code>IFile.InMemoryReader</code> to read map-outputs present in-memory.
	 */
	public static class InMemoryReader<K, V> extends Reader<K, V> {
		RamManager ramManager;
		TaskAttemptID taskAttemptId;

		public InMemoryReader(RamManager ramManager,
				TaskAttemptID taskAttemptId, byte[] data, int start, int length)
				throws IOException {
			super(null, null, length - start, null, null);
			this.ramManager = ramManager;
			this.taskAttemptId = taskAttemptId;

			buffer = data;
			bufferSize = (int) fileLength;
			dataIn.reset(buffer, start, length);
		}

		@Override
		public long getPosition() throws IOException {
			// InMemoryReader does not initialize streams like Reader, so
			// in.getPos()
			// would not work. Instead, return the number of uncompressed bytes
			// read,
			// which will be correct since in-memory data is not compressed.
			return bytesRead;
		}

		@Override
		public long getLength() {
			return fileLength;
		}

		private void dumpOnError() {
			File dumpFile = new File("../output/" + taskAttemptId + ".dump");
			System.err.println("Dumping corrupt map-output of " + taskAttemptId
					+ " to " + dumpFile.getAbsolutePath());
			try {
				FileOutputStream fos = new FileOutputStream(dumpFile);
				fos.write(buffer, 0, bufferSize);
				fos.close();
			} catch (IOException ioe) {
				System.err.println("Failed to dump map-output of "
						+ taskAttemptId);
			}
		}

		public boolean next(DataInputBuffer key, DataInputBuffer value)
				throws IOException {
			try {
				// Sanity check
				if (eof) {
					throw new EOFException("Completed reading " + bytesRead);
				}

				// Read key and value lengths
				int oldPos = dataIn.getPosition();
				int keyLength = WritableUtils.readVInt(dataIn);
				int valueLength = WritableUtils.readVInt(dataIn);
				int pos = dataIn.getPosition();
				bytesRead += pos - oldPos;

				// Check for EOF
				if (keyLength == EOF_MARKER && valueLength == EOF_MARKER) {
					eof = true;
					return false;
				}

				// Sanity check
				if (keyLength < 0) {
					throw new IOException("Rec# " + recNo
							+ ": Negative key-length: " + keyLength);
				}
				if (valueLength < 0) {
					throw new IOException("Rec# " + recNo
							+ ": Negative value-length: " + valueLength);
				}

				final int recordLength = keyLength + valueLength;

				// Setup the key and value
				pos = dataIn.getPosition();
				byte[] data = dataIn.getData();
				key.reset(data, pos, keyLength);
				value.reset(data, (pos + keyLength), valueLength);

				// Position for the next record
				long skipped = dataIn.skip(recordLength);
				if (skipped != recordLength) {
					throw new IOException("Rec# " + recNo
							+ ": Failed to skip past record of length: "
							+ recordLength);
				}

				// Record the byte
				bytesRead += recordLength;

				++recNo;

				return true;
			} catch (IOException ioe) {
				dumpOnError();
				throw ioe;
			}
		}

		public void close() {
			// Release
			dataIn = null;
			buffer = null;

			// Inform the RamManager
			ramManager.unreserve(bufferSize);
		}
	}

	public static class InMemoryTrippleReader<T1, T2, T3> extends
			TrippleReader<T1, T2, T3> {
		RamManager ramManager;
		TaskAttemptID taskAttemptId;
		boolean dummyfile = false;

		public InMemoryTrippleReader(RamManager ramManager,
				TaskAttemptID taskAttemptId, byte[] data, int start, int length)
				throws IOException {
			super(null, null, length - start, null, null);
			this.ramManager = ramManager;
			this.taskAttemptId = taskAttemptId;

			buffer = data;
			bufferSize = (int) fileLength;
			dataIn.reset(buffer, start, length);

			if (length <= 3)
				dummyfile = true;
		}

		@Override
		public long getPosition() throws IOException {
			// InMemoryReader does not initialize streams like Reader, so
			// in.getPos()
			// would not work. Instead, return the number of uncompressed bytes
			// read,
			// which will be correct since in-memory data is not compressed.
			return bytesRead;
		}

		@Override
		public long getLength() {
			return fileLength;
		}

		private void dumpOnError() {
			File dumpFile = new File("../output/" + taskAttemptId + ".dump");
			System.err.println("Dumping corrupt map-output of " + taskAttemptId
					+ " to " + dumpFile.getAbsolutePath());
			try {
				FileOutputStream fos = new FileOutputStream(dumpFile);
				fos.write(buffer, 0, bufferSize);
				fos.close();
			} catch (IOException ioe) {
				System.err.println("Failed to dump map-output of "
						+ taskAttemptId);
			}
		}

		public boolean next(DataInputBuffer t1, DataInputBuffer t2,
				DataInputBuffer t3) throws IOException {
			try {
				// Sanity check
				if (eof) {
					throw new EOFException("Completed reading " + bytesRead);
				}

				if (dummyfile)
					return false;

				// Read key and value lengths
				int oldPos = dataIn.getPosition();
				int t1Length = WritableUtils.readVInt(dataIn);
				int t2Length = WritableUtils.readVInt(dataIn);
				int t3Length = WritableUtils.readVInt(dataIn);
				int pos = dataIn.getPosition();
				bytesRead += pos - oldPos;

				// Check for EOF
				if (t1Length == EOF_MARKER && t2Length == EOF_MARKER
						&& t3Length == EOF_MARKER) {
					eof = true;
					return false;
				}

				// Sanity check
				if (t1Length < 0) {
					throw new IOException("Rec# " + recNo
							+ ": Negative t1-length: " + t1Length);
				}
				if (t2Length < 0) {
					throw new IOException("Rec# " + recNo
							+ ": Negative t2-length: " + t2Length);
				}
				if (t3Length < 0) {
					throw new IOException("Rec# " + recNo
							+ ": Negative t3-length: " + t3Length);
				}

				final int recordLength = t1Length + t2Length + t3Length;

				// Setup the key and value
				pos = dataIn.getPosition();
				byte[] data = dataIn.getData();
				t1.reset(data, pos, t1Length);
				t2.reset(data, (pos + t1Length), t2Length);
				t3.reset(data, (pos + t1Length + t2Length), t3Length);

				// Position for the next record
				long skipped = dataIn.skip(recordLength);
				if (skipped != recordLength) {
					throw new IOException("Rec# " + recNo
							+ ": Failed to skip past record of length: "
							+ recordLength);
				}

				// Record the byte
				bytesRead += recordLength;

				++recNo;

				return true;
			} catch (IOException ioe) {
				dumpOnError();
				throw ioe;
			}
		}

		public void close() {
			// Release
			dataIn = null;
			buffer = null;

			// Inform the RamManager
			ramManager.unreserve(bufferSize);
		}
	}

	public static class BatchPreserveFile<KEY extends Object, VALUE extends Object> {
		private static final int EOF_MARKER = -1;
		private static final int DEFAULT_BUFFER_SIZE = 128 * 1024;

		boolean eof = false;
		int bufferSize = DEFAULT_BUFFER_SIZE;

		Class<KEY> keyClass;
		Class<VALUE> valClass;
		Serializer<KEY> keySerializer;
		Serializer<VALUE> valSerializer;

		Deserializer<KEY> keyDeserializer;
		Deserializer<VALUE> valDeserializer;
		DataOutputBuffer buffer = new DataOutputBuffer();
		private TreeMap<KEY, VALUE> cache;

		DataInputBuffer keyIn = new DataInputBuffer();
		DataInputBuffer valueIn = new DataInputBuffer();

		RandomAccessFile dataFile;
		RandomAccessFile tempdataFile;
		SerializationFactory serializationFactory;

		public BatchPreserveFile(Configuration conf, Path datafile,
				Class<KEY> keyClass, Class<VALUE> valClass) throws IOException {

			FileSystem fs = FileSystem.getLocal(conf);
			if (!fs.exists(datafile)) {
				fs.create(datafile);
				eof = true;
			}
			this.dataFile = new RandomAccessFile(datafile.toString(), "rw");
			this.tempdataFile = new RandomAccessFile(datafile.toString()
					+ ".temp", "rw");
			this.keyClass = keyClass;
			this.valClass = valClass;

			serializationFactory = new SerializationFactory(conf);
			this.keySerializer = serializationFactory.getSerializer(keyClass);
			this.keySerializer.open(buffer);
			this.valSerializer = serializationFactory.getSerializer(valClass);
			this.valSerializer.open(buffer);
			//
			// Deserializer
			this.keyDeserializer = serializationFactory
					.getDeserializer(keyClass);
			keyDeserializer.open(keyIn);
			this.valDeserializer = serializationFactory
					.getDeserializer(valClass);
			valDeserializer.open(valueIn);

			byte b1[], b2[];
			cache = new TreeMap<KEY, VALUE>();
			while (!eof) {
				int len1 = WritableUtils.readVInt(dataFile);
				int len2 = WritableUtils.readVInt(dataFile);
				if (EOF_MARKER == len1 && EOF_MARKER == len2) {
					eof = true;
					break;
				} else {
					KEY key = null;
					VALUE value = null;
					b1 = new byte[len1];
					b2 = new byte[len2];
					dataFile.read(b1, 0, len1);
					dataFile.read(b2, 0, len2);
					keyIn.reset(b1, 0, len1);
					valueIn.reset(b2, 0, len2);
					key = keyDeserializer.deserialize(key);
					value = valDeserializer.deserialize(value);
					// Log.info(key+"  "+value);
					cache.put(key, value);
				}
			}
		}

		public void close() throws IOException {
			if (tempdataFile.length() > 0) {
				byte b1[], b2[];
				cache = new TreeMap<KEY, VALUE>();
				tempdataFile.seek(0);
				while (true) {
					int len1 = WritableUtils.readVInt(tempdataFile);
					int len2 = WritableUtils.readVInt(tempdataFile);
					if (EOF_MARKER == len1 && EOF_MARKER == len2) {
						break;
					} else {
						KEY key = null;
						VALUE value = null;
						b1 = new byte[len1];
						b2 = new byte[len2];
						tempdataFile.read(b1, 0, len1);
						tempdataFile.read(b2, 0, len2);
						keyIn.reset(b1, 0, len1);
						valueIn.reset(b2, 0, len2);
						key = keyDeserializer.deserialize(key);
						value = valDeserializer.deserialize(value);
						cache.put(key, value);
					}
				}

			}
			dataFile.setLength(0);
			for (Map.Entry<KEY, VALUE> entry : cache.entrySet()) {
				keySerializer.serialize(entry.getKey());
				int keyLength = buffer.getLength();
				if (keyLength < 0) {
					throw new IOException("Negative key-length not allowed: "
							+ keyLength + " for " + entry.getKey());
				}
				valSerializer.serialize(entry.getValue());
				int valLength = buffer.getLength() - keyLength;
				if (valLength < 0) {
					throw new IOException("Negative value-length not allowed: "
							+ valLength + " for " + entry.getValue());
				}
				WritableUtils.writeVInt(dataFile, keyLength);
				WritableUtils.writeVInt(dataFile, valLength);
				dataFile.write(buffer.getData(), 0, buffer.getLength());
				buffer.reset();
			}
			WritableUtils.writeVInt(dataFile, EOF_MARKER);
			WritableUtils.writeVInt(dataFile, EOF_MARKER);

			tempdataFile.close();
			dataFile.close();
			valDeserializer.close();
			keyDeserializer.close();
			keySerializer.close();
			valSerializer.close();
		}

		public void put(KEY key, VALUE value) throws IOException {
			if (tempdataFile.length() > 0)
				tempdataFile.seek(tempdataFile.length() - 2);
			keySerializer.serialize(key);
			int keyLength = buffer.getLength();
			if (keyLength < 0) {
				throw new IOException("Negative key-length not allowed: "
						+ keyLength + " for " + key);
			}
			valSerializer.serialize(value);
			int valLength = buffer.getLength() - keyLength;
			if (valLength < 0) {
				throw new IOException("Negative t4-length not allowed: "
						+ valLength + " for " + value);
			}
			WritableUtils.writeVInt(tempdataFile, keyLength);
			WritableUtils.writeVInt(tempdataFile, valLength);
			tempdataFile.write(buffer.getData(), 0, buffer.getLength());
			buffer.reset();
			WritableUtils.writeVInt(tempdataFile, EOF_MARKER);
			WritableUtils.writeVInt(tempdataFile, EOF_MARKER);
		}

		public boolean containsKey(KEY key) {
			return cache.containsKey(key);
		}

		public VALUE get(KEY key) {
			if (cache.containsKey(key))
				return cache.get(key);
			else
				return null;
		}
	}

	public static class PreserveFile<KEY extends Object, VALUE extends Object, SOURCE extends Object, OUTVALUE extends Object> {

		long countSeekIO, countWriteIO, needReadBytes, bufferReadBytes;

		private static final int EOF_MARKER = -1;
		private static final int DEFAULT_READBUFFER_SIZE = 128 * 1024;
		private static final int DEFAULT_WRITEBUFFER_SIZE = 128 * 1024;
		private static final int DEFAULT_BUFFER_SIZE = 128 * 1024;
		private static final int MAX_VINT_SIZE = 3 * 9;
		private static final int INDEX_ENTRY_SIZE = 16;
		private static final int RESULT_KV_LABEL = -100;
		private int buf_num;
		private int bufferType;
		private int bufferinterval;
		private int num_reduce;
		private int preserveWriteSize;
		private int preserveReadSize;
		private ArrayList<Integer> keyset;
		private LinkedList<Interval> keyIntervalSingle;

		private TreeMap<Integer, LinkedList<Interval>> intervalmap;
		private TreeMap<Integer, ArrayList<Integer>> keyMap;
		private TreeMap<Integer, ArrayList<Integer>> keyMap2;

		public enum TYPE {
			FILEEND, RECORDEND, MORE
		}

		class DataCache {
			private byte buffer[];
			public long buf_start;
			public long buf_end;
			public long cur_pos;
			private RandomAccessFile file;
			public long len;

			public DataCache() {
				buffer = new byte[preserveReadSize];
				buf_start = 0;
				buf_end = 0;
				len = 0;
			}

			public DataCache(RandomAccessFile file) {
				this.file = file;
				buffer = new byte[preserveWriteSize];
				buf_start = 0;
				buf_end = 0;
				cur_pos = 0;
				len = 0;
			}

			public DataInputBuffer seekKey(int keyhash,
					RandomAccessFile dataFile) throws IOException {

				int pos = indexCache.get(keyhash).offset;
				int entrylength = indexCache.get(keyhash).length;

				if ((pos + entrylength < buf_end) && (pos >= buf_start)) {
					dataIn.reset(buffer, (int) (pos - buf_start),
							(int) (buf_end - pos));
				} else {
					countSeekIO++;
					dataFile.seek(pos);
					while (preserveReadSize < entrylength) {
						preserveReadSize *= 2;
					}
					buffer = null;
					buffer = new byte[preserveReadSize];
					int n = dataFile.read(buffer, 0, preserveReadSize);
					buf_start = indexCache.get(keyhash).offset;
					buf_end = buf_start + n;
					dataIn.reset(buffer, 0, n);

					len += n;
					bufferReadBytes += n;
				}
				return dataIn;
			}

			public DataInputBuffer seekKeyMultiple(int keyhash,
					RandomAccessFile dataFile) throws IOException {
				int pos = indexCache.get(keyhash).offset;
				int entrylength = indexCache.get(keyhash).length;
				int num = indexCache.get(keyhash).buf_num;

				if ((pos + entrylength <= buf_end) && (pos >= buf_start)) {
					dataIn.reset(buffer, (int) (pos - buf_start), entrylength);
				} else {
					// Log.info("find :  num :  "+num + " " +keyhash+" pos  " +
					// pos + "  len " + entrylength + " buf_start "+ buf_start
					// +" buf_end "+buf_end);
					countSeekIO++;
					Interval interval = intervalmap.get(num).poll();
					// Log.info(interval.toString() + " =  " + keyhash);
					buffer = new byte[interval.interval];

					dataFile.seek(pos);
					int n = dataFile.read(buffer, 0, interval.interval);
					buf_start = pos;
					buf_end = buf_start + n;
					dataIn.reset(buffer, 0, entrylength);

					len += n;
					bufferReadBytes += n;
				}
				return dataIn;
			}

			public DataInputBuffer seekKeySingle(int keyhash,
					RandomAccessFile dataFile) throws IOException {
				int pos = indexCache.get(keyhash).offset;
				int entrylength = indexCache.get(keyhash).length;

				if ((pos + entrylength <= buf_end) && (pos >= buf_start)) {
					dataIn.reset(buffer, (int) (pos - buf_start), entrylength);
				} else {
					// Log.info("find : "+keyhash+" pos  " + pos + "  len " +
					// entrylength + " buf_start "+ buf_start
					// +" buf_end "+buf_end);
					countSeekIO++;
					Interval interval = keyIntervalSingle.poll();
					// Log.info(interval.toString() + " =  " + keyhash);
					buffer = new byte[interval.interval];

					dataFile.seek(pos);
					int n = dataFile.read(buffer, 0, interval.interval);
					buf_start = pos;
					buf_end = buf_start + n;
					dataIn.reset(buffer, 0, entrylength);

					len += n;
					bufferReadBytes += n;
				}
				return dataIn;
			}

			public void put(DataOutputBuffer out) throws IOException {
				if (cur_pos + out.getLength() >= preserveWriteSize) {
					countWriteIO++;
					file.seek(file.length());
					file.write(buffer, 0, (int) cur_pos);
					cur_pos = 0;
				}
				if (out.getLength() >= preserveWriteSize) {
					file.seek(file.length());
					file.write(out.getData(), 0, out.getLength());
				} else {
					System.arraycopy(out.getData(), 0, buffer, (int) cur_pos,
							out.getLength());
					cur_pos += out.getLength();
				}
			}

			public void close() throws IOException {
				countWriteIO++;
				file.seek(file.length());
				file.write(buffer, 0, (int) cur_pos);
				cur_pos = 0;
			}
		}

		class IndexEntry {
			int offset;
			int length;
			int buf_num;

			public IndexEntry(int offset, int length, int buf_num) {
				this.offset = offset;
				this.length = length;
				this.buf_num = buf_num;
			}

			public IndexEntry(int offset, int length) {
				this.offset = offset;
				this.length = length;
			}

			@Override
			public String toString() {
				return "index entry: " + offset + "\t" + length + "\t"
						+ buf_num;
			}
		}

		// Count records read from disk
		private long numRecordsRead = 0;

		DataCache singleRead;
		ArrayList<DataCache> buf_pool;
		DataCache outDataFile, outAppendFile;
		RandomAccessFile dataFile;
		RandomAccessFile appendFile;
		FSDataInputStream indexIns;
		FSDataOutputStream indexOut;

		long readbytes = 0;
		long fileLength;
		boolean eof = false;
		boolean indexend = false;

		private byte[] databuffer = null;
		private byte[] indexbuffer = null;
		private int bufferSize = DEFAULT_BUFFER_SIZE;
		private int indexbufferSize = DEFAULT_BUFFER_SIZE;
		private DataInputBuffer dataIn = new DataInputBuffer();
		private DataInputBuffer indexIn = new DataInputBuffer();

		int bytesWritten = 0;

		// Count records written to disk
		private long numRecordsWritten = 0;

		// <hashcode, <offset, length>>
		private TreeMap<Integer, IndexEntry> indexCache = new TreeMap<Integer, IndexEntry>();

		Class<KEY> t1Class;
		Class<VALUE> t2Class;
		Class<SOURCE> t3Class;
		Class<OUTVALUE> t4Class;
		Serializer<KEY> t1Serializer;
		Serializer<VALUE> t2Serializer;
		Serializer<SOURCE> t3Serializer;
		Serializer<OUTVALUE> t4Serializer;

		DataOutputBuffer buffer = new DataOutputBuffer();

		KEY cachedKey;
		int cachedOffset;
		int recNo = 1;

		public PreserveFile(Configuration conf, Path datafile,
				Path oldindexfile, Path newindexfile, /** int iteration, */
				Class<KEY> t1Class, Class<VALUE> t2Class,
				Class<SOURCE> t3Class, Class<OUTVALUE> t4Class)
				throws IOException {
			FileSystem fs = FileSystem.getLocal(conf);
			this.dataFile = new RandomAccessFile(datafile.toString(), "rw");
			File tmpFile = new File(datafile.toString() + "-append.tmp");
			if (tmpFile.exists()) {
				tmpFile.delete();
				// Log.info("delete file " + tmpFile);
			}
			this.appendFile = new RandomAccessFile(datafile.toString()
					+ "-append.tmp", "rw");

			this.t1Class = t1Class;
			this.t2Class = t2Class;
			this.t3Class = t3Class;
			this.t4Class = t4Class;
			SerializationFactory serializationFactory = new SerializationFactory(
					conf);
			this.t1Serializer = serializationFactory.getSerializer(t1Class);
			this.t1Serializer.open(buffer);
			this.t2Serializer = serializationFactory.getSerializer(t2Class);
			this.t2Serializer.open(buffer);
			this.t3Serializer = serializationFactory.getSerializer(t3Class);
			this.t3Serializer.open(buffer);
			this.t4Serializer = serializationFactory.getSerializer(t4Class);
			this.t4Serializer.open(buffer);

			indexOut = fs.create(newindexfile);
			this.fileLength = fs.getFileStatus(datafile).getLen();

			if (conf != null) {
				// bufferSize = conf.getInt("io.file.buffer.size",
				// DEFAULT_BUFFER_SIZE);
				preserveWriteSize = conf.getInt(
						"mapred.incremental.preservewrite.buffersize",
						DEFAULT_WRITEBUFFER_SIZE);
				preserveReadSize = conf.getInt(
						"mapred.incremental.preserveread.buffersize",
						DEFAULT_READBUFFER_SIZE);
				bufferSize = DEFAULT_BUFFER_SIZE;
			}

			outAppendFile = new DataCache(appendFile);
			outDataFile = new DataCache(dataFile);
			// Log.info("read buffer size " + bufferSize);

			databuffer = new byte[bufferSize];
			dataIn.reset(databuffer, 0, 0);

			long start = System.currentTimeMillis();
			// incrmental start, no oldindexfile
			if (oldindexfile != null) {
				indexIns = fs.open(oldindexfile);

				// total = indexIns.readInt();
				buf_num = (indexIns.readInt() + 1);
				indexbuffer = new byte[indexbufferSize];
				indexIn.reset(indexbuffer, 0, 0);

				// load the indices
				while (true) {
					if (indexIn.getLength() - indexIn.getPosition() < INDEX_ENTRY_SIZE) {
						if (indexend)
							break;

						readIndexNextBlock(INDEX_ENTRY_SIZE);
					}

					int hashcode = indexIn.readInt();
					int offset = indexIn.readInt();
					int length = indexIn.readInt();
					int buf_num = indexIn.readInt();

					indexCache.put(hashcode, new IndexEntry(offset, length,
							buf_num));
					// Log.info("put  hashcode: " + hashcode + "\toffset: " +
					// offset + "\tlength: " + length+"\tbuf_num: "+buf_num);
				}
				buf_pool = new ArrayList<DataCache>();
				for (int i = 0; i <= buf_num; i++) {
					DataCache buf = new DataCache();
					buf_pool.add(buf);
				}

				indexIn.close();
				indexbuffer = null;
			} else {
				buf_num = 0;
			}
		}

		public PreserveFile(Configuration conf, Path datafile,
				Path oldindexfile, Path newindexfile, /** int iteration, */
				Class<KEY> t1Class, Class<VALUE> t2Class,
				Class<SOURCE> t3Class, Class<OUTVALUE> t4Class, int buffertype , int bufferInterval )
				throws IOException {
			this.bufferType = buffertype % 10;
			this.bufferinterval = bufferInterval;
			this.num_reduce = buffertype / 10;
			
			FileSystem fs = FileSystem.getLocal(conf);
			this.dataFile = new RandomAccessFile(datafile.toString(), "rw");
			File tmpFile = new File(datafile.toString() + "-append.tmp");
			if (tmpFile.exists()) {
				tmpFile.delete();
				// Log.info("delete file " + tmpFile);
			}
			this.appendFile = new RandomAccessFile(datafile.toString()
					+ "-append.tmp", "rw");

			this.t1Class = t1Class;
			this.t2Class = t2Class;
			this.t3Class = t3Class;
			this.t4Class = t4Class;
			SerializationFactory serializationFactory = new SerializationFactory(
					conf);
			this.t1Serializer = serializationFactory.getSerializer(t1Class);
			this.t1Serializer.open(buffer);
			this.t2Serializer = serializationFactory.getSerializer(t2Class);
			this.t2Serializer.open(buffer);
			this.t3Serializer = serializationFactory.getSerializer(t3Class);
			this.t3Serializer.open(buffer);
			this.t4Serializer = serializationFactory.getSerializer(t4Class);
			this.t4Serializer.open(buffer);

			indexOut = fs.create(newindexfile);
			this.fileLength = fs.getFileStatus(datafile).getLen();

			if (conf != null) {
				// bufferSize = conf.getInt("io.file.buffer.size",
				// DEFAULT_BUFFER_SIZE);
				preserveWriteSize = conf.getInt(
						"mapred.incremental.preservewrite.buffersize",
						DEFAULT_WRITEBUFFER_SIZE);
				preserveReadSize = conf.getInt(
						"mapred.incremental.preserveread.buffersize",
						DEFAULT_READBUFFER_SIZE);
				bufferSize = DEFAULT_BUFFER_SIZE;
			}

			singleRead = new DataCache();
			outAppendFile = new DataCache(appendFile);
			outDataFile = new DataCache(dataFile);
			// Log.info("read buffer size " + bufferSize);

			databuffer = new byte[bufferSize];
			dataIn.reset(databuffer, 0, 0);

			if (oldindexfile != null) {
				indexIns = fs.open(oldindexfile);

				// total = indexIns.readInt();
				buf_num = (indexIns.readInt() + 1);
				indexbuffer = new byte[indexbufferSize];
				indexIn.reset(indexbuffer, 0, 0);

				// load the indices
				while (true) {
					if (indexIn.getLength() - indexIn.getPosition() < INDEX_ENTRY_SIZE) {
						if (indexend)
							break;

						readIndexNextBlock(INDEX_ENTRY_SIZE);
					}

					int hashcode = indexIn.readInt();
					int offset = indexIn.readInt();
					int length = indexIn.readInt();
					int buf_num = indexIn.readInt();

					indexCache.put(hashcode, new IndexEntry(offset, length,
							buf_num));
					// Log.info("more put  hashcode: " + hashcode + "\toffset: "
					// +
					// offset + "\tlength: " + length+"\tbuf_num: "+buf_num);
				}
				buf_pool = new ArrayList<DataCache>();
				if (buffertype > 2) {

					intervalmap = new TreeMap<Integer, LinkedList<Interval>>();
					keyMap = new TreeMap<Integer, ArrayList<Integer>>();
					keyMap2 = new TreeMap<Integer, ArrayList<Integer>>();

					for (int i = 0; i < buf_num; i++) {

						ArrayList<Integer> list = new ArrayList<Integer>();
						keyMap.put(i, list);
						intervalmap.put(i, new LinkedList<Interval>());

						DataCache buf = new DataCache();
						buf_pool.add(buf);

					}
				} else if (buffertype == 1 || buffertype == 2) {
					keyIntervalSingle = new LinkedList<Interval>();
				}

				indexIn.close();
				indexbuffer = null;
			} else {
				buf_num = 0;
			}
		}

		public void setKeyset(ArrayList<Integer> keyset) {
			this.keyset = keyset;
		}

		static class Interval {
			@Override
			public String toString() {
				return "Interval [beginkey=" + beginkey + ", beginPos="
						+ beginPos + ", endkey=" + endkey + ", interval="
						+ interval + "]";
			}

			int beginkey;
			int beginPos;
			int endkey;
			int interval;
		}

		public void analyseKeySingle() {
			keyIntervalSingle = new LinkedList<Interval>();
			long start = System.currentTimeMillis();
			int beginKey = -1, num = -1, end = -1, keycount = 0;
			int count = 1;

			for (int key : keyset) {
				Interval i = new Interval();
				if (beginKey == -1 && end == -1) {
					end = beginKey = key;
					num = indexCache.get(key).buf_num;
				} else if (beginKey == -1) {
					beginKey = end;
					num = indexCache.get(end).buf_num;
				}
				if (keycount++ > 2048 || num != indexCache.get(key).buf_num) {
					i.beginkey = beginKey;
					i.endkey = end;
					i.beginPos = indexCache.get(beginKey).offset;
					i.interval = (indexCache.get(end).offset
							+ indexCache.get(end).length - i.beginPos);

					keyIntervalSingle.add(i);
					beginKey = -1;
					keycount = 0;

					// Log.info(i.toString());
				}
				end = key;

				if (count++ == keyset.size()) {
					i = new Interval();
					if (beginKey != -1) {
						// Log.info(beginKey + "  end   "+
						// keyset.size()+"   count  " + count+ "  "+ end );
						i.beginkey = beginKey;
						i.endkey = end;
						i.beginPos = indexCache.get(beginKey).offset;
						i.interval = (indexCache.get(end).offset
								+ indexCache.get(end).length - i.beginPos);

						keyIntervalSingle.add(i);
						// Log.info(i.toString()+"  end ");
					} else {
						// Log.info(beginKey + "  end   "+
						// keyset.size()+"   count  " + count+ "  "+ end );
						i.beginkey = end;
						i.endkey = end;
						i.beginPos = indexCache.get(end).offset;
						i.interval = indexCache.get(end).length;

						keyIntervalSingle.add(i);
						// Log.info(i.toString()+"  end ");
					}
				}
			}
			Log.info("analyse Key Single takes time :"
					+ (System.currentTimeMillis() - start) + "     "
					+ keyIntervalSingle.size());
		}

		public void analyseKeyMultiple() {
			long start = System.currentTimeMillis();
			int num = -1, allentrylen = 0;
			for (int i = 0; i <= buf_num; i++) {
				keyMap.put(i, new ArrayList<Integer>());
				keyMap2.put(i, new ArrayList<Integer>());
				intervalmap.put(i, new LinkedList<Interval>());
			}
			for (int key : keyset) {
				num = indexCache.get(key).buf_num;
				keyMap2.get(num).add(key);
			}

			for (int i = 0; i < buf_num; i++) {
				allentrylen = 0;
				for (int key : keyMap2.get(i)) {
					keyMap.get(i).add(key);
					keyMap.get(i).add(allentrylen);
					allentrylen += indexCache.get(key).length;
				}
				// Log.info(i + " keyMap  : " + keyMap.get(i));
				// Log.info(i + " keyMap2 : " + keyMap2.get(i));
			}

			for (int i = 0; i < buf_num; i++) {
				int preLen = 0, preEntryLen = 0;
				for (int j = 0; j < keyMap.get(i).size();) {
					preLen = indexCache.get(keyMap.get(i).get(j)).offset;
					preEntryLen = keyMap.get(i).get(j + 1);

					boolean b = false;
					int end = 0;

					for (int k = j + 2; k < keyMap.get(i).size() && !b; k += 2) {
						if (((indexCache.get(keyMap.get(i).get(k)).offset
								+ indexCache.get(keyMap.get(i).get(k)).length - preLen) > bufferinterval
								* (keyMap.get(i).get(k + 1) - preEntryLen + indexCache
										.get(keyMap.get(i).get(k)).length))
								|| (indexCache.get(keyMap.get(i).get(k)).offset
										+ indexCache.get(keyMap.get(i).get(k)).length - preLen) > 1024 * 1024) {
							end = k - 2;
							b = true;
							// Log.info(keyMap.get(i).get(k)+" offset "+
							// indexCache.get(keyMap.get(i).get(k)).offset+"  entrylen "+
							// indexCache.get(keyMap.get(i).get(k)).length
							// +" prelen  "+ preLen + "  "+ keyMap.get(i).get(k
							// + 1) +" preEntryLen "+ preEntryLen);
						}
					}

					if (end < j) {
						end = keyMap.get(i).size() - 2;
					}

					Interval interval = new Interval();
					interval.beginkey = keyMap.get(i).get(j);
					interval.beginPos = indexCache.get(interval.beginkey).offset;
					interval.endkey = keyMap.get(i).get(end);
					interval.interval = (indexCache.get(interval.endkey).offset
							+ indexCache.get(interval.endkey).length - interval.beginPos);

					intervalmap.get(i).add(interval);

					// Log.info("begin key : "+ keyMap.get(i).get(j) + "  "+
					// keyMap.get(i).get(j+ 1) +" preLen : " + preLen
					// +"  preEntryLen : "
					// + preEntryLen + " end key : " + keyMap.get(i).get(end));
					// Log.info("num "+ i + " " +interval );
					j = end + 2;
				}
			}
			// for (int i = 0; i < buf_num; i++) {
			// Log.info("num : "+ i +"   interval size  has :  " +
			// intervalmap.get(i).size() );
			// }
			Log.info("analyse Key Multiple takes time :"
					+ (System.currentTimeMillis() - start));

		}

		public void analyseKeyMultiple2() {
			long start = System.currentTimeMillis();
			for (int i = 0; i < buf_num; i++) {
				keyMap.put(i, new ArrayList<Integer>());
				intervalmap.put(i, new LinkedList<Interval>());
			}
			for (int key : keyset) {

				keyMap.get(indexCache.get(key).buf_num).add(key);
			}
//			for (int i = 0; i < buf_num; i++) {
//				Log.info("num :: " + i + "   " + keyMap.get(i));
//			}

			int intervallen = bufferinterval * num_reduce;
//			System.out.println(bufferinterval + "   " + num_reduce);
			for (int i = 0; i < buf_num; i++) {

				for (int j = 0; j < keyMap.get(i).size();) {
					int end = -1;
					boolean b = false;

					for (int k = j + 1; k < keyMap.get(i).size() && !b; k++) {
						if ((keyMap.get(i).get(k) - keyMap.get(i).get(j)) > intervallen
								* (k - j)
								 ||  k - j > 1024) {
							b = true;
							end = k - 1;
						}
					}

					if (end < j)
						end = keyMap.get(i).size() - 1;

					Interval interval = new Interval();
					interval.beginkey = keyMap.get(i).get(j);
					interval.beginPos = indexCache.get(interval.beginkey).offset;
					interval.endkey = keyMap.get(i).get(end);
					interval.interval = (indexCache.get(interval.endkey).offset
							+ indexCache.get(interval.endkey).length - interval.beginPos);

					intervalmap.get(i).add(interval);

					j = end + 1;

					// Log.info(i+"  "+interval);
				}
			}

			// for (int i = 0; i < buf_num; i++) {
			// Log.info("num : "+ i +"   interval size  has :  " +
			// intervalmap.get(i).size() );
			// }
			Log.info("analyse Key Multiple2 takes time :"
					+ (System.currentTimeMillis() - start));
		}

		public void analyseKeyMultiple3() {
			long start = System.currentTimeMillis();
			int num = -1;
			for (int i = 0; i <= buf_num; i++) {
				keyMap.put(i, new ArrayList<Integer>());
				keyMap2.put(i, new ArrayList<Integer>());
				intervalmap.put(i, new LinkedList<Interval>());
			}
			for (int key : keyset) {
				num = indexCache.get(key).buf_num;
				keyMap2.get(num).add(key);
			}

			for (int i = 0; i < buf_num; i++) {
				for (int key : keyMap2.get(i)) {
					keyMap.get(i).add(key);
					keyMap.get(i).add(indexCache.get(key).offset);
				}
			}

			for (int i = 0; i < buf_num; i++) {
				for (int j = 0; j < keyMap.get(i).size();) {
					boolean b = false;
					int end = 0;

					for (int k = j + 2; k < keyMap.get(i).size() && !b; k += 2) {
						if ((keyMap.get(i).get(k + 1)
								- keyMap.get(i).get(k - 1) > bufferinterval)
								|| keyMap.get(i).get(k + 1)
										- keyMap.get(i).get(j + 1) > 2048000) {
							end = k - 2;
							b = true;
						}
					}

					if (end < j) {
						end = keyMap.get(i).size() - 2;
					}

					Interval interval = new Interval();
					interval.beginkey = keyMap.get(i).get(j);
					interval.beginPos = indexCache.get(interval.beginkey).offset;
					interval.endkey = keyMap.get(i).get(end);
					interval.interval = (indexCache.get(interval.endkey).offset
							+ indexCache.get(interval.endkey).length - interval.beginPos);

//					Log.info("num : " + i + "   " + interval);
					intervalmap.get(i).add(interval);

					j = end + 2;
				}
			}
			Log.info("analyse Key Multiple2 takes time :"
					+ (System.currentTimeMillis() - start));
		}

		boolean hasflush = false;

		public void flush() throws IOException {
			if (!hasflush) {

				outDataFile.close();
				outAppendFile.close();

				appendFile.seek(0);
				if (dataFile.length() >= 3) {
					dataFile.seek(dataFile.length() - 3);
				} else
					dataFile.seek(dataFile.length());
				int len;
				byte[] buf = new byte[128 * 1024];
				while ((len = appendFile.read(buf)) > 0) {
					dataFile.write(buf, 0, len);
				}

				WritableUtils.writeVInt(dataFile, EOF_MARKER);
				WritableUtils.writeVInt(dataFile, EOF_MARKER);
				WritableUtils.writeVInt(dataFile, EOF_MARKER);

				if (bufferType > 3) {
					if (keyset == null)
						keyset = new ArrayList<Integer>();
					keyset.clear();
				}

				indexOut.writeInt(buf_num);
				for (Map.Entry<Integer, IndexEntry> entry : indexCache
						.entrySet()) {
					if (bufferType > 3)
						keyset.add(entry.getKey());
					indexOut.writeInt(entry.getKey());
					indexOut.writeInt(entry.getValue().offset);
					indexOut.writeInt(entry.getValue().length);
					indexOut.writeInt(entry.getValue().buf_num);
				}
				indexOut.flush();
			}
			// indexOut.close();
			// indexOut = null;
			hasflush = true;
		}

		public void close() throws IOException {
			System.out.println("total io is seek io " + countSeekIO
					+ " write io " + countWriteIO + " read record bytes "
					+ needReadBytes + " buffer has read " + bufferReadBytes);

			if (bufferType > 2 && buf_num > 0) {
				System.out.println("cachetype " + bufferType + " buf_num "
						+ buf_num);
				for (int i = 0; i < buf_num; i++) {
					System.out.println("buffer " + i + " read bytes "
							+ buf_pool.get(i).len);
				}
			}
			flush();
			// Release the buffer
			dataIn = null;
			databuffer = null;

			// Close the serializers
			t1Serializer.close();
			t2Serializer.close();
			t3Serializer.close();
			t4Serializer.close();

			indexOut.close();
			indexOut = null;

			dataFile.close();
			dataFile = null;
			appendFile.close();
			appendFile = null;
		}

		private int getHashcode(KEY t1) {
			// if there is a hash conflict, then try next hashcode
			int keyhash = t1.hashCode();
			while (indexCache.containsKey(keyhash)) {
				// Log.info("hash conflict " + t1);
				keyhash = String.valueOf(keyhash).hashCode();
			}

			return keyhash;
		}

		public void appendShuffleKVS(KEY t1, VALUE t2, SOURCE t3)
				throws IOException {
			if (bufferType == 0) {
				appendShuffleKVSNo(t1, t2, t3);
			} else {
				appendShuffleKVSMore(t1, t2, t3);
			}
		}

		public void appendShuffleKVSMore(KEY t1, VALUE t2, SOURCE t3)
				throws IOException {
			if (t1.getClass() != t1Class)
				throw new IOException("wrong t1 class: " + t1.getClass()
						+ " is not " + t1Class);

			if (t2.getClass() != t2Class)
				throw new IOException("wrong t2 class: " + t2.getClass()
						+ " is not " + t2Class);
			if (t3.getClass() != t3Class)
				throw new IOException("wrong t3 class: " + t3.getClass()
						+ " is not " + t3Class);

			if (!t1.equals(cachedKey)) {
				cachedKey = t1;
				cachedOffset = (int) dataFile.length()
						+ (int) outDataFile.cur_pos;
			}

			// Append the 'key'
			t1Serializer.serialize(t1);
			int t1Length = buffer.getLength();
			if (t1Length < 0) {
				throw new IOException("Negative key-length not allowed: "
						+ t1Length + " for " + t1);
			}

			// Append the 'key'
			t2Serializer.serialize(t2);
			int t2Length = buffer.getLength() - t1Length;
			if (t2Length < 0) {
				throw new IOException("Negative t2-length not allowed: "
						+ t2Length + " for " + t2);
			}

			// Append the 'value'
			t3Serializer.serialize(t3);
			int t3Length = buffer.getLength() - t2Length - t1Length;
			if (t3Length < 0) {
				throw new IOException("Negative t3-length not allowed: "
						+ t3Length + " for " + t3);
			}

			// Write the record out
			DataOutputBuffer temp = new DataOutputBuffer();
			WritableUtils.writeVInt(temp, t1Length);
			WritableUtils.writeVInt(temp, t2Length);
			WritableUtils.writeVInt(temp, t3Length);
			outDataFile.put(temp);
			temp.reset();
			// WritableUtils.writeVInt(dataFile, t1Length); // key length
			// WritableUtils.writeVInt(dataFile, t2Length); // key length
			// WritableUtils.writeVInt(dataFile, t3Length); // value length

			/********************************
			 * same key length and same value length, need optimize later
			 */

			// Log.info("key length : " + keyLength + " value length : " +
			// valueLength);
			// dataFile.write(buffer.getData(), 0, buffer.getLength()); // data
			outDataFile.put(buffer);
			// Reset
			buffer.reset();

			// Update bytes written
			bytesWritten += t1Length + t2Length + t3Length
					+ WritableUtils.getVIntSize(t1Length)
					+ WritableUtils.getVIntSize(t2Length)
					+ WritableUtils.getVIntSize(t3Length);

			++numRecordsWritten;
		}

		public void appendShuffleKVSNo(KEY t1, VALUE t2, SOURCE t3)
				throws IOException {
			countWriteIO++;
			if (t1.getClass() != t1Class)
				throw new IOException("wrong t1 class: " + t1.getClass()
						+ " is not " + t1Class);

			if (t2.getClass() != t2Class)
				throw new IOException("wrong t2 class: " + t2.getClass()
						+ " is not " + t2Class);
			if (t3.getClass() != t3Class)
				throw new IOException("wrong t3 class: " + t3.getClass()
						+ " is not " + t3Class);

			if (!t1.equals(cachedKey)) {
				cachedKey = t1;
				cachedOffset = (int) dataFile.length();
			}

			// Append the 'key'
			t1Serializer.serialize(t1);
			int t1Length = buffer.getLength();
			if (t1Length < 0) {
				throw new IOException("Negative key-length not allowed: "
						+ t1Length + " for " + t1);
			}

			// Append the 'key'
			t2Serializer.serialize(t2);
			int t2Length = buffer.getLength() - t1Length;
			if (t2Length < 0) {
				throw new IOException("Negative t2-length not allowed: "
						+ t2Length + " for " + t2);
			}

			// Append the 'value'
			t3Serializer.serialize(t3);
			int t3Length = buffer.getLength() - t2Length - t1Length;
			if (t3Length < 0) {
				throw new IOException("Negative t3-length not allowed: "
						+ t3Length + " for " + t3);
			}

			// Write the record out
			WritableUtils.writeVInt(dataFile, t1Length); // key length
			WritableUtils.writeVInt(dataFile, t2Length); // key length
			WritableUtils.writeVInt(dataFile, t3Length); // value length

			/********************************
			 * same key length and same value length, need optimize later
			 */

			dataFile.write(buffer.getData(), 0, buffer.getLength()); // data

			// Reset
			buffer.reset();

			// Update bytes written
			bytesWritten += t1Length + t2Length + t3Length
					+ WritableUtils.getVIntSize(t1Length)
					+ WritableUtils.getVIntSize(t2Length)
					+ WritableUtils.getVIntSize(t3Length);

			++numRecordsWritten;
		}

		// appendResKV must be involked after appendShuffleKVS
		public void appendResKV(KEY t1, OUTVALUE t4) throws IOException {
			if (bufferType == 0) {
				appendResKVNo(t1, t4);
			} else {
				appendResKVMore(t1, t4);
			}
		}

		public void appendResKVMore(KEY t1, OUTVALUE t4) throws IOException {
			if (t1.getClass() != t1Class)
				throw new IOException("wrong t1 class: " + t1.getClass()
						+ " is not " + t1Class);

			if (t4.getClass() != t4Class)
				throw new IOException("wrong t4 class: " + t4.getClass()
						+ " is not " + t4Class);

			// Append the 'key'
			t1Serializer.serialize(t1);
			int t1Length = buffer.getLength();
			if (t1Length < 0) {
				throw new IOException("Negative key-length not allowed: "
						+ t1Length + " for " + t1);
			}

			// Append the 'rvalue'
			t4Serializer.serialize(t4);
			int t4Length = buffer.getLength() - t1Length;
			if (t4Length < 0) {
				throw new IOException("Negative t4-length not allowed: "
						+ t4Length + " for " + t4);
			}

			// Write the record out
			DataOutputBuffer temp = new DataOutputBuffer();
			WritableUtils.writeVInt(temp, t1Length);
			WritableUtils.writeVInt(temp, t4Length);
			WritableUtils.writeVInt(temp, RESULT_KV_LABEL);
			outDataFile.put(temp);
			temp.reset();
			// WritableUtils.writeVInt(dataFile, RESULT_KV_LABEL);
			// WritableUtils.writeVInt(dataFile, t1Length); // key length
			// WritableUtils.writeVInt(dataFile, t4Length); // key length
			// WritableUtils.writeVInt(dataFile, RESULT_KV_LABEL); // value
			// length

			/********************************
			 * same key length and same value length, need optimize later
			 */

			// Log.info("key length : " + keyLength + " value length : " +
			// valueLength);
			// dataFile.write(buffer.getData(), 0, buffer.getLength()); // data
			outDataFile.put(buffer);
			// Reset
			buffer.reset();

			// Update bytes written
			bytesWritten += t1Length + t4Length
					+ WritableUtils.getVIntSize(t1Length)
					+ WritableUtils.getVIntSize(t4Length)
					+ WritableUtils.getVIntSize(RESULT_KV_LABEL);
			++numRecordsWritten;

			if (!t1.equals(cachedKey)) {
				throw new IOException(t1 + "is not the key " + cachedKey
						+ " that should be!");
			} else {
				int hash = getHashcode(t1);
				indexCache.put(hash, new IndexEntry(cachedOffset, bytesWritten,
						buf_num));
				// Log.info("appned " + t1 + " hashkey " + hash + "\toffset " +
				// cachedOffset + "\tlength " + bytesWritten+"\tbuf_num: "+0);

				cachedOffset = -1;
				bytesWritten = 0;
			}
		}

		public void appendResKVNo(KEY t1, OUTVALUE t4) throws IOException {
			countWriteIO++;
			if (t1.getClass() != t1Class)
				throw new IOException("wrong t1 class: " + t1.getClass()
						+ " is not " + t1Class);

			if (t4.getClass() != t4Class)
				throw new IOException("wrong t4 class: " + t4.getClass()
						+ " is not " + t4Class);

			// Append the 'key'
			t1Serializer.serialize(t1);
			int t1Length = buffer.getLength();
			if (t1Length < 0) {
				throw new IOException("Negative key-length not allowed: "
						+ t1Length + " for " + t1);
			}

			// Append the 'rvalue'
			t4Serializer.serialize(t4);
			int t4Length = buffer.getLength() - t1Length;
			if (t4Length < 0) {
				throw new IOException("Negative t4-length not allowed: "
						+ t4Length + " for " + t4);
			}

			// Write the record out
			WritableUtils.writeVInt(dataFile, t1Length); // key length
			WritableUtils.writeVInt(dataFile, t4Length); // key length
			WritableUtils.writeVInt(dataFile, RESULT_KV_LABEL); // value length

			/********************************
			 * same key length and same value length, need optimize later
			 */

			dataFile.write(buffer.getData(), 0, buffer.getLength()); // data

			// Reset
			buffer.reset();

			// Update bytes written
			bytesWritten += t1Length + t4Length
					+ WritableUtils.getVIntSize(t1Length)
					+ WritableUtils.getVIntSize(t4Length)
					+ WritableUtils.getVIntSize(RESULT_KV_LABEL);
			++numRecordsWritten;

			if (!t1.equals(cachedKey)) {
				throw new IOException(t1 + "is not the key " + cachedKey
						+ " that should be!");
			} else {
				int hash = getHashcode(t1);
				indexCache.put(hash, new IndexEntry(cachedOffset, bytesWritten,
						buf_num));

				// Log.info("appned " + t1 + " hashkey " + hash + "\toffset " +
				// cachedOffset + "\tlength " + bytesWritten);

				cachedOffset = -1;
				bytesWritten = 0;
			}
		}

		public void updateShuffleKVS(int keyhash, KEY t1, VALUE t2, SOURCE t3)
				throws IOException {
			if (bufferType == 0) {
				updateShuffleKVSNo(keyhash, t1, t2, t3);
			} else {
				updateShuffleKVSMore(keyhash, t1, t2, t3);
			}
		}

		public void updateShuffleKVSMore(int keyhash, KEY t1, VALUE t2,
				SOURCE t3) throws IOException {
			if (t1.getClass() != t1Class)
				throw new IOException("wrong t1 class: " + t1.getClass()
						+ " is not " + t1Class);

			if (t2.getClass() != t2Class)
				throw new IOException("wrong t2 class: " + t2.getClass()
						+ " is not " + t2Class);
			if (t3.getClass() != t3Class)
				throw new IOException("wrong t3 class: " + t3.getClass()
						+ " is not " + t3Class);

			// if not the cached key, that means t1 is a new key, we should
			// update the index cache,
			// or else, we do nothing
			if (!t1.equals(cachedKey)) {
				cachedKey = t1;
				cachedOffset = (int) (appendFile.length() + dataFile.length()
						- 3 + (int) outAppendFile.cur_pos);
				// Log.info("put index cache " + t1 + " hashkey " + keyhash +
				// "\toffset " + (appendFile.length() + dataFile.length() - 3));
			}

			// Append the 'key'
			t1Serializer.serialize(t1);
			int t1Length = buffer.getLength();
			if (t1Length < 0) {
				throw new IOException("Negative key-length not allowed: "
						+ t1Length + " for " + t1);
			}

			// Append the 'key'
			t2Serializer.serialize(t2);
			int t2Length = buffer.getLength() - t1Length;
			if (t2Length < 0) {
				throw new IOException("Negative t2-length not allowed: "
						+ t2Length + " for " + t2);
			}

			// Append the 'value'
			t3Serializer.serialize(t3);
			int t3Length = buffer.getLength() - t2Length - t1Length;
			if (t3Length < 0) {
				throw new IOException("Negative t3-length not allowed: "
						+ t3Length + " for " + t3);
			}

			// Write the record out
			DataOutputBuffer temp = new DataOutputBuffer();
			WritableUtils.writeVInt(temp, t1Length);
			WritableUtils.writeVInt(temp, t2Length);
			WritableUtils.writeVInt(temp, t3Length);
			outAppendFile.put(temp);
			temp.reset();
			// WritableUtils.writeVInt(appendFile, t1Length); // key length
			// WritableUtils.writeVInt(appendFile, t2Length); // key length
			// WritableUtils.writeVInt(appendFile, t3Length); // value length

			/********************************
			 * same key length and same value length, need optimize later
			 */

			// Log.info("key length : " + keyLength + " value length : " +
			// valueLength);
			// appendFile.write(buffer.getData(), 0, buffer.getLength()); //
			// data
			outAppendFile.put(buffer);
			// Reset
			buffer.reset();

			// Update bytes written
			bytesWritten += t1Length + t2Length + t3Length
					+ WritableUtils.getVIntSize(t1Length)
					+ WritableUtils.getVIntSize(t2Length)
					+ WritableUtils.getVIntSize(t3Length);
			++numRecordsWritten;
		}

		public void updateShuffleKVSNo(int keyhash, KEY t1, VALUE t2, SOURCE t3)
				throws IOException {
			countWriteIO++;
			if (t1.getClass() != t1Class)
				throw new IOException("wrong t1 class: " + t1.getClass()
						+ " is not " + t1Class);

			if (t2.getClass() != t2Class)
				throw new IOException("wrong t2 class: " + t2.getClass()
						+ " is not " + t2Class);
			if (t3.getClass() != t3Class)
				throw new IOException("wrong t3 class: " + t3.getClass()
						+ " is not " + t3Class);

			// if not the cached key, that means t1 is a new key, we should
			// update the index cache,
			// or else, we do nothing
			if (!t1.equals(cachedKey)) {
				cachedKey = t1;
				cachedOffset = (int) (appendFile.length() + dataFile.length() - 3);
				// Log.info("put index cache " + t1 + " hashkey " + keyhash +
				// "\toffset " + (appendFile.length() + dataFile.length() - 3));
			}

			// Append the 'key'
			t1Serializer.serialize(t1);
			int t1Length = buffer.getLength();
			if (t1Length < 0) {
				throw new IOException("Negative key-length not allowed: "
						+ t1Length + " for " + t1);
			}

			// Append the 'key'
			t2Serializer.serialize(t2);
			int t2Length = buffer.getLength() - t1Length;
			if (t2Length < 0) {
				throw new IOException("Negative t2-length not allowed: "
						+ t2Length + " for " + t2);
			}

			// Append the 'value'
			t3Serializer.serialize(t3);
			int t3Length = buffer.getLength() - t2Length - t1Length;
			if (t3Length < 0) {
				throw new IOException("Negative t3-length not allowed: "
						+ t3Length + " for " + t3);
			}

			// Write the record out
			WritableUtils.writeVInt(appendFile, t1Length); // key length
			WritableUtils.writeVInt(appendFile, t2Length); // key length
			WritableUtils.writeVInt(appendFile, t3Length); // value length

			/********************************
			 * same key length and same value length, need optimize later
			 */

			// Log.info("key length : " + keyLength + " value length : " +
			// valueLength);
			appendFile.write(buffer.getData(), 0, buffer.getLength()); // data

			// Reset
			buffer.reset();

			// Update bytes written
			bytesWritten += t1Length + t2Length + t3Length
					+ WritableUtils.getVIntSize(t1Length)
					+ WritableUtils.getVIntSize(t2Length)
					+ WritableUtils.getVIntSize(t3Length);
			++numRecordsWritten;
		}

		public void updateResKV(int keyhash, KEY t1, OUTVALUE t4)
				throws IOException {
			if (bufferType == 0) {
				updateResKVNo(keyhash, t1, t4);
			} else {
				updateResKVMore(keyhash, t1, t4);
			}
		}

		public void updateResKVMore(int keyhash, KEY t1, OUTVALUE t4)
				throws IOException {
			if (t1.getClass() != t1Class)
				throw new IOException("wrong t1 class: " + t1.getClass()
						+ " is not " + t1Class);

			if (t4.getClass() != t4Class)
				throw new IOException("wrong t4 class: " + t4.getClass()
						+ " is not " + t4Class);

			// Append the 'key'
			t1Serializer.serialize(t1);
			int t1Length = buffer.getLength();
			if (t1Length < 0) {
				throw new IOException("Negative key-length not allowed: "
						+ t1Length + " for " + t1);
			}

			// Append the 'outvalue'
			t4Serializer.serialize(t4);
			int t4Length = buffer.getLength() - t1Length;
			if (t4Length < 0) {
				throw new IOException("Negative t4-length not allowed: "
						+ t4Length + " for " + t4);
			}

			// Write the record out
			DataOutputBuffer temp = new DataOutputBuffer();
			WritableUtils.writeVInt(temp, t1Length);
			WritableUtils.writeVInt(temp, t4Length);
			WritableUtils.writeVInt(temp, RESULT_KV_LABEL);
			outAppendFile.put(temp);
			temp.close();
			// WritableUtils.writeVInt(appendFile, t1Length); // key length
			// WritableUtils.writeVInt(appendFile, t4Length); // key length
			// WritableUtils.writeVInt(appendFile, RESULT_KV_LABEL); // value
			// length

			/********************************
			 * same key length and same value length, need optimize later
			 */

			// Log.info("key length : " + keyLength + " value length : " +
			// valueLength);
			// appendFile.write(buffer.getData(), 0, buffer.getLength()); //
			// data
			outAppendFile.put(buffer);
			// Reset
			buffer.reset();

			// Update bytes written
			bytesWritten += t1Length + t4Length
					+ WritableUtils.getVIntSize(t1Length)
					+ WritableUtils.getVIntSize(t4Length)
					+ WritableUtils.getVIntSize(RESULT_KV_LABEL);
			++numRecordsWritten;

			// updateShuffleKVS followed by updateResKV
			if (!t1.equals(cachedKey)) {
				throw new IOException(t1 + "is not the key " + cachedKey
						+ " that should be!");
			} else {
				indexCache.put(keyhash, new IndexEntry(cachedOffset,
						bytesWritten, buf_num));
				// Log.info("update " + t1 + " hashkey " + keyhash + "\toffset "
				// + cachedOffset + "\tlength " +
				// bytesWritten+"\tbuf_num "+buf_num);
				cachedOffset = -1;
				bytesWritten = 0;
			}
		}

		public void updateResKVNo(int keyhash, KEY t1, OUTVALUE t4)
				throws IOException {
			countWriteIO++;
			if (t1.getClass() != t1Class)
				throw new IOException("wrong t1 class: " + t1.getClass()
						+ " is not " + t1Class);

			if (t4.getClass() != t4Class)
				throw new IOException("wrong t4 class: " + t4.getClass()
						+ " is not " + t4Class);

			// Append the 'key'
			t1Serializer.serialize(t1);
			int t1Length = buffer.getLength();
			if (t1Length < 0) {
				throw new IOException("Negative key-length not allowed: "
						+ t1Length + " for " + t1);
			}

			// Append the 'outvalue'
			t4Serializer.serialize(t4);
			int t4Length = buffer.getLength() - t1Length;
			if (t4Length < 0) {
				throw new IOException("Negative t4-length not allowed: "
						+ t4Length + " for " + t4);
			}

			// Write the record out
			WritableUtils.writeVInt(appendFile, t1Length); // key length
			WritableUtils.writeVInt(appendFile, t4Length); // key length
			WritableUtils.writeVInt(appendFile, RESULT_KV_LABEL); // value
																	// length

			/********************************
			 * same key length and same value length, need optimize later
			 */

			// Log.info("key length : " + keyLength + " value length : " +
			// valueLength);
			appendFile.write(buffer.getData(), 0, buffer.getLength()); // data

			// Reset
			buffer.reset();

			// Update bytes written
			bytesWritten += t1Length + t4Length
					+ WritableUtils.getVIntSize(t1Length)
					+ WritableUtils.getVIntSize(t4Length)
					+ WritableUtils.getVIntSize(RESULT_KV_LABEL);
			++numRecordsWritten;

			// updateShuffleKVS followed by updateResKV
			if (!t1.equals(cachedKey)) {
				throw new IOException(t1 + "is not the key " + cachedKey
						+ " that should be!");
			} else {
				indexCache.put(keyhash, new IndexEntry(cachedOffset,
						bytesWritten, buf_num));

				// Log.info("update " + t1 + " hashkey " + keyhash + "\toffset "
				// + cachedOffset + "\tlength " + bytesWritten);

				cachedOffset = -1;
				bytesWritten = 0;
			}
		}

		public void removeKV(int keyhash, KEY t1) throws IOException {
			if (!t1.equals(cachedKey)) {
				throw new IOException(t1 + "is not the key " + cachedKey
						+ " that should be!");
			} else {
				// update the index cache, the removed kv's index is -1
				indexCache.put(keyhash, new IndexEntry(-1, -1));
				cachedOffset = -1;
				bytesWritten = 0;
			}
		}

		public long getLength() throws IOException {
			return appendFile.length();
		}

		/**
		 * Read upto len bytes into buf starting at offset off.
		 * 
		 * @param buf
		 *            buffer
		 * @param off
		 *            offset
		 * @param len
		 *            length of buffer
		 * @return the no. of bytes read
		 * @throws IOException
		 */
		private int readIndexData(byte[] buf, int off, int len)
				throws IOException {
			int bytesRead = 0;
			while (bytesRead < len) {
				int n = indexIns.read(buf, off + bytesRead, len - bytesRead);
				if (n < 0) {
					indexend = true;
					return bytesRead;
				}
				bytesRead += n;
			}
			return len;
		}

		private int readData(byte[] buf, int off, int len) throws IOException {
			int bytesRead = 0;
			while (bytesRead < len) {
				int n = dataFile.read(buf, off + bytesRead, len - bytesRead);
				if (n < 0) {
					return bytesRead;
				}
				bytesRead += n;
			}
			return len;
		}

		int readNextBlock(int minSize) throws IOException {
			int n = rejigData(databuffer,
					(bufferSize < minSize) ? new byte[minSize << 1]
							: databuffer);
			bufferSize = databuffer.length;
			return n;
		}

		void readIndexNextBlock(int minSize) throws IOException {
			indexbuffer = rejigIndexData(indexbuffer,
					(indexbufferSize < minSize) ? new byte[minSize << 1]
							: indexbuffer);
			indexbufferSize = indexbuffer.length;
		}

		private int rejigData(byte[] source, byte[] destination)
				throws IOException {
			// Copy remaining data into the destination array
			int bytesRemaining = dataIn.getLength() - dataIn.getPosition();
			if (bytesRemaining > 0) {
				System.arraycopy(source, dataIn.getPosition(), destination, 0,
						bytesRemaining);
			}

			/*
			 * Log.info("dataIn.getLength() " + dataIn.getLength() +
			 * " dataIn.getPosition() " + dataIn.getPosition() + " remaining: "
			 * + bytesRemaining + " (destination.length - bytesRemaining) " +
			 * (destination.length - bytesRemaining));
			 */

			// Read as much data as will fit from the underlying stream
			int n = readData(destination, bytesRemaining,
					(destination.length - bytesRemaining));
			dataIn.reset(destination, 0, (bytesRemaining + n));

			return n;
		}

		private byte[] rejigIndexData(byte[] source, byte[] destination)
				throws IOException {
			// Copy remaining data into the destination array
			int bytesRemaining = indexIn.getLength() - indexIn.getPosition();
			if (bytesRemaining > 0) {
				System.arraycopy(source, indexIn.getPosition(), destination, 0,
						bytesRemaining);
			}

			// Read as much data as will fit from the underlying stream
			int n = readIndexData(destination, bytesRemaining,
					(destination.length - bytesRemaining));
			indexIn.reset(destination, 0, (bytesRemaining + n));

			return destination;
		}

		public boolean seekKey(KEY key, int rehash, IntWritable hashcode)
				throws IOException {

			if (bufferType > 2) {
				seekKeyMultiple(key, rehash, hashcode);
			} else if (bufferType == 0) {
				seekKeyNo(key, rehash, hashcode);
			} else {
				seekKeySingle(key, rehash, hashcode);
			}
			// Log.info("seek  key :  "+key +"hashcode "+hashcode);
			return true;
		}

		public boolean seekKeyMultiple(KEY key, int rehash, IntWritable hashcode)
				throws IOException {
			if (rehash != 0)
				Log.info("rehash " + rehash);

			int keyhash = key.hashCode();
			for (int i = 0; i < rehash; i++) {
				keyhash = String.valueOf(keyhash).hashCode();
			}

			if (indexCache.get(keyhash) == null
					|| indexCache.get(keyhash).offset == -1) {
				return false;
			}

			if (bufferType > 3 && buf_num > 1) {
				if (!hasanalysekey) {
					if (bufferType == 4) {
						analyseKeyMultiple2();
					} else if (bufferType == 5) {
						analyseKeyMultiple();
					} else {
						analyseKeyMultiple3();
					}
					hasanalysekey = true;
				}
				dataIn = buf_pool.get(indexCache.get(keyhash).buf_num)
						.seekKeyMultiple(keyhash, dataFile);
			} else {
				dataIn = buf_pool.get(indexCache.get(keyhash).buf_num).seekKey(
						keyhash, dataFile);
			}
			// Log.info("seek keyhash " + keyhash);
			hashcode.set(keyhash);
			needReadBytes += indexCache.get(keyhash).length;
			return true;
		}

		boolean hasanalysekey = false;

		public void setHasanalysekey(boolean hasanalysekey) {
			this.hasanalysekey = hasanalysekey;
		}

		public boolean seekKeySingle(KEY key, int rehash, IntWritable hashcode)
				throws IOException {
			if (rehash != 0)
				Log.info("rehash " + rehash);

			int keyhash = key.hashCode();
			for (int i = 0; i < rehash; i++) {
				keyhash = String.valueOf(keyhash).hashCode();
			}

			if (indexCache.get(keyhash) == null
					|| indexCache.get(keyhash).offset == -1) {
				return false;
			}

			if (bufferType == 2) {
				if (!hasanalysekey) {
					analyseKeySingle();
					hasanalysekey = true;
				}
				dataIn = singleRead.seekKeySingle(keyhash, dataFile);
			}
			if (bufferType == 1)
				dataIn = singleRead.seekKey(keyhash, dataFile);

			// Log.info("seek keyhash " + keyhash);
			hashcode.set(keyhash);
			needReadBytes += indexCache.get(keyhash).length;
			return true;
		}

		public boolean seekKeyNo(KEY key, int rehash, IntWritable hashcode)
				throws IOException {
			countSeekIO++;
			if (rehash != 0)
				Log.info("rehash " + rehash);

			int keyhash = key.hashCode();
			for (int i = 0; i < rehash; i++) {
				keyhash = String.valueOf(keyhash).hashCode();
			}

			if (indexCache.get(keyhash) == null) {
				// Log.info("no hashkey " + keyhash + " for key " + key +
				// " found!");
				return false;
			}

			// Log.info("key " + key + " offset " + indexCache.get(keyhash) +
			// " file length " + dataFile.length());
			dataFile.seek(indexCache.get(keyhash).offset);
			// Log.info("supposed offset " + indexCache.get(keyhash) +
			// "\toffset prev " + dataFile.getFilePointer());
			// int n = readData(databuffer, 0, bufferSize);
			int entrylength = indexCache.get(keyhash).length;
			if (entrylength <= bufferSize - MAX_VINT_SIZE) {
				int n = dataFile.read(databuffer, 0, entrylength
						+ MAX_VINT_SIZE);
				// Log.info("read index entry length " + entrylength +
				// " actually read " + n);
				dataIn.reset(databuffer, 0, n);
			} else {
				Log.info("this is a big key, which is " + key + " length "
						+ entrylength);

				int n = dataFile.read(databuffer, 0, bufferSize);
				// Log.info("read index entry length " + entrylength +
				// " actually read " + n);
				dataIn.reset(databuffer, 0, n);
			}

			hashcode.set(keyhash);
			needReadBytes += indexCache.get(keyhash).length;
			return true;
		}

		public boolean seekKey(int keyhash) throws IOException {
			if (bufferType == 0) {
				seekKeyNo(keyhash);
			} else if (bufferType == 1) {
				dataIn = singleRead.seekKey(keyhash, dataFile);
			} else if (bufferType == 2) {
				if (!hasanalysekey) {
					analyseKeySingle();
					hasanalysekey = true;
				}
				dataIn = singleRead.seekKey(keyhash, dataFile);
			} else if (bufferType == 3) {
				dataIn = buf_pool.get(indexCache.get(keyhash).buf_num).seekKey(
						keyhash, dataFile);
			} else if (bufferType == 4) {
				if (!hasanalysekey) {
					analyseKeyMultiple2();
					hasanalysekey = true;
				}
				dataIn = buf_pool.get(indexCache.get(keyhash).buf_num)
						.seekKeyMultiple(keyhash, dataFile);
			} else if (bufferType == 5) {
				if (!hasanalysekey) {
					analyseKeyMultiple();
					hasanalysekey = true;
				}
				dataIn = buf_pool.get(indexCache.get(keyhash).buf_num)
						.seekKeyMultiple(keyhash, dataFile);
			} else {
				if (!hasanalysekey) {
					analyseKeyMultiple();
					hasanalysekey = true;
				}
				dataIn = buf_pool.get(indexCache.get(keyhash).buf_num)
						.seekKeyMultiple(keyhash, dataFile);
			}

			// if (bufferType == 0) {
			// seekKeyNo(keyhash);
			// } else if (bufferType == 1 ||bufferType == 2) {
			// dataIn = singleRead.seekKey(keyhash, dataFile);
			// } else {
			// dataIn = buf_pool.get(indexCache.get(keyhash).buf_num).seekKey(
			// keyhash, dataFile);
			// }
			return true;
		}

		public boolean seekKeyNo(int keyhash) throws IOException {
			countWriteIO++;
			dataFile.seek(indexCache.get(keyhash).offset);
			int entrylength = indexCache.get(keyhash).length;
			if (entrylength <= bufferSize - MAX_VINT_SIZE) {
				int n = dataFile.read(databuffer, 0, entrylength
						+ MAX_VINT_SIZE);
				dataIn.reset(databuffer, 0, n);
			} else {
				int n = dataFile.read(databuffer, 0, bufferSize);
				dataIn.reset(databuffer, 0, n);
			}
			return true;
		}

		public TYPE next(DataInputBuffer t1, DataInputBuffer t2,
				DataInputBuffer t3, DataInputBuffer t4) throws IOException {
			// if ((dataIn.getLength() - dataIn.getPosition()) < 15) {
			// int n = readNextBlock(15);
			// }

			int oldPos = dataIn.getPosition();
			// Log.info("oldpos   "+oldPos+"    len   "+dataIn.getLength());
			int t1Length = WritableUtils.readVInt(dataIn);
			int t2Length = WritableUtils.readVInt(dataIn);
			int t3Length = WritableUtils.readVInt(dataIn);
			int pos = dataIn.getPosition();

			readbytes += pos - oldPos;

			if (t3Length == RESULT_KV_LABEL) {
				final int recordLength = t1Length + t2Length;

				if ((dataIn.getLength() - pos) < recordLength) {
					int n = readNextBlock(recordLength);
					if ((dataIn.getLength() - dataIn.getPosition()) < recordLength) {
						throw new EOFException("Rec# " + recNo
								+ ": Could read the next " + " record");
					}
				}

				pos = dataIn.getPosition();
				byte[] data = dataIn.getData();
				t1.reset(data, pos, t1Length);
				t4.reset(data, (pos + t1Length), t2Length);

				long skipped = dataIn.skip(recordLength);
				if (skipped != recordLength) {
					throw new IOException("Rec# " + recNo
							+ ": Failed to skip past record " + "of length: "
							+ recordLength);
				}

				readbytes += recordLength;

				++recNo;
				++numRecordsRead;

				return TYPE.RECORDEND;
			}

			if (t1Length == EOF_MARKER && t2Length == EOF_MARKER
					&& t3Length == EOF_MARKER) {
				return TYPE.FILEEND;
			}
			if (t1Length < 0) {
				throw new IOException("Rec# " + recNo
						+ ": Negative t1-length: " + t1Length);
			}
			if (t2Length < 0) {
				throw new IOException("Rec# " + recNo
						+ ": Negative t2-length: " + t2Length);
			}
			if (t3Length < 0) {
				throw new IOException("Rec# " + recNo
						+ ": Negative t3-length: " + t3Length);
			}

			final int recordLength = t1Length + t2Length + t3Length;
			if ((dataIn.getLength() - pos) < recordLength) {

				readNextBlock(recordLength);
				if ((dataIn.getLength() - dataIn.getPosition()) < recordLength) {
					throw new EOFException("Rec# " + recNo
							+ ": Could read the next " + " record");
				}
			}
			pos = dataIn.getPosition();
			byte[] data = dataIn.getData();
			t1.reset(data, pos, t1Length);
			t2.reset(data, (pos + t1Length), t2Length);
			t3.reset(data, (pos + t1Length + t2Length), t3Length);

			long skipped = dataIn.skip(recordLength);
			if (skipped != recordLength) {
				throw new IOException("Rec# " + recNo
						+ ": Failed to skip past record " + "of length: "
						+ recordLength);
			}
			readbytes += recordLength;

			++recNo;
			++numRecordsRead;

			// Log.info("next dataIn pos " + pos + " dataIn len " +
			// dataIn.getLength() + " t1len " + t1Length + " t2len " + t2Length
			// + " t3len " + t3Length);

			return TYPE.MORE;
		}

		public void writeResult(RecordWriter resultOut, JobConf job)
				throws IOException {
			DataInputBuffer keyIn1 = new DataInputBuffer();
			DataInputBuffer valueIn1 = new DataInputBuffer();
			DataInputBuffer skeyIn1 = new DataInputBuffer();
			DataInputBuffer outvalueIn1 = new DataInputBuffer();

			SerializationFactory serializationFactory = new SerializationFactory(
					job);
			Deserializer<KEY> keyDeserializer1 = serializationFactory
					.getDeserializer(t1Class);
			keyDeserializer1.open(keyIn1);
			Deserializer<VALUE> valDeserializer1 = serializationFactory
					.getDeserializer(t2Class);
			valDeserializer1.open(valueIn1);
			Deserializer<SOURCE> skeyDeserializer1 = serializationFactory
					.getDeserializer(t3Class);
			skeyDeserializer1.open(skeyIn1);
			Deserializer<OUTVALUE> outvalDeserializer1 = serializationFactory
					.getDeserializer(t4Class);
			outvalDeserializer1.open(outvalueIn1);

			KEY key1 = null;
			VALUE value1 = null;
			SOURCE source1 = null;
			OUTVALUE outvalue1 = null;

			flush();
			buf_num++;
			hasanalysekey = false;
			buf_pool = new ArrayList<DataCache>();
			for (int i = 0; i < buf_num; i++) {
				DataCache buf = new DataCache();
				buf_pool.add(buf);

			}
			for (Map.Entry<Integer, PreserveFile<KEY, VALUE, SOURCE, OUTVALUE>.IndexEntry> entry : indexCache
					.entrySet()) {
				int hashcode = entry.getKey();
				seekKey(hashcode);
				if (indexCache.get(hashcode).offset
						+ indexCache.get(hashcode).length < dataFile.length()) {
					while (this.next(keyIn1, valueIn1, skeyIn1, outvalueIn1) != PreserveFile.TYPE.RECORDEND) {
					}
					key1 = keyDeserializer1.deserialize(key1);
					outvalue1 = outvalDeserializer1.deserialize(outvalue1);
					resultOut.write((KEY) key1, outvalue1);
					// System.out.println(key1 + "          " + outvalue1);
				}
			}
		}

	}
}
