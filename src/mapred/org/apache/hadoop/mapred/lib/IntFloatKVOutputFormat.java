package org.apache.hadoop.mapred.lib;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.mortbay.log.Log;

public class IntFloatKVOutputFormat extends FileOutputFormat<IntWritable, FloatWritable> {
	  protected static class IntFloatLineRecordWriter implements RecordWriter<IntWritable, FloatWritable> {
		    private static final String utf8 = "UTF-8";
		    private static final byte[] newline;
		    static {
		      try {
		        newline = "\n".getBytes(utf8);
		      } catch (UnsupportedEncodingException uee) {
		        throw new IllegalArgumentException("can't find " + utf8 + " encoding");
		      }
		    }

		    protected DataOutputStream out;
		    private final byte[] keyValueSeparator;

		    public IntFloatLineRecordWriter(DataOutputStream out, String keyValueSeparator) {
		      this.out = out;
		      try {
		        this.keyValueSeparator = keyValueSeparator.getBytes(utf8);
		      } catch (UnsupportedEncodingException uee) {
		        throw new IllegalArgumentException("can't find " + utf8 + " encoding");
		      }
		    }

		    public IntFloatLineRecordWriter(DataOutputStream out) {
		      this(out, "\t");
		    }

		    public synchronized void write(IntWritable key, FloatWritable value)
		      throws IOException {

		      boolean nullKey = key == null;
		      boolean nullValue = value == null;
		      if (nullKey || nullValue) {
		        throw new IOException("output key and value cannot be null!");
		      }
		      
		      out.writeInt(key.get());
		      out.write(keyValueSeparator);
		      out.writeFloat(value.get());
		      Log.info(key.get() + "\t" + value.get());
		      out.write(newline);
		    }

		    public synchronized void close(Reporter reporter) throws IOException {
		      out.close();
		    }
		  }

		  public RecordWriter<IntWritable, FloatWritable> getRecordWriter(FileSystem ignored,
		                                                  JobConf job,
		                                                  String name,
		                                                  Progressable progress)
		    throws IOException {
		    boolean isCompressed = getCompressOutput(job);
		    String keyValueSeparator = job.get("mapred.textoutputformat.separator", 
		                                       "\t");
		    if (!isCompressed) {
		      Path file = FileOutputFormat.getTaskOutputPath(job, name);
		      FileSystem fs = file.getFileSystem(job);
		      FSDataOutputStream fileOut = fs.create(file, progress);
		      return new IntFloatLineRecordWriter(fileOut, keyValueSeparator);
		    } else {
		      Class<? extends CompressionCodec> codecClass =
		        getOutputCompressorClass(job, GzipCodec.class);
		      // create the named codec
		      CompressionCodec codec = ReflectionUtils.newInstance(codecClass, job);
		      // build the filename including the extension
		      Path file = 
		        FileOutputFormat.getTaskOutputPath(job, 
		                                           name + codec.getDefaultExtension());
		      FileSystem fs = file.getFileSystem(job);
		      FSDataOutputStream fileOut = fs.create(file, progress);
		      return new IntFloatLineRecordWriter(new DataOutputStream
		                                        (codec.createOutputStream(fileOut)),
		                                        keyValueSeparator);
		    }
		  }
}
