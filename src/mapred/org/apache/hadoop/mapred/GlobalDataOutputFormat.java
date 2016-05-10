package org.apache.hadoop.mapred;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

public class GlobalDataOutputFormat<K, V> extends FileOutputFormat<K, V> {

	  protected static class GlobalDataRecordWriter<K, V>
	    implements RecordWriter<K, V> {
	    private static final String utf8 = "UTF-8";

	    protected DataOutputStream out;
	    private final byte[] keyValueSeparator;
	    private final byte[] recordSeparator;			
	    private final byte[] recordKVSeparator;

	    public GlobalDataRecordWriter(DataOutputStream out, String keyValueSeparator) {
	      this.out = out;
	      try {
	        this.keyValueSeparator = keyValueSeparator.getBytes(utf8);
	        this.recordSeparator = "#".getBytes(utf8);
	        this.recordKVSeparator = ":".getBytes(utf8);
	      } catch (UnsupportedEncodingException uee) {
	        throw new IllegalArgumentException("can't find " + utf8 + " encoding");
	      }
	      
	      try {
			writeObject("GlobalKey" + keyValueSeparator);
	      } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
	      }
	    }

	    public GlobalDataRecordWriter(DataOutputStream out) {
	      this(out, "\t");
	    }

	    /**
	     * Write the object to the byte stream, handling Text as a special
	     * case.
	     * @param o the object to print
	     * @throws IOException if the write throws, we pass it on
	     */
	    private void writeObject(Object o) throws IOException {
	      if (o instanceof Text) {
	        Text to = (Text) o;
	        out.write(to.getBytes(), 0, to.getLength());
	      } else {
	        out.write(o.toString().getBytes(utf8));
	        
	      }
	    }

	    public synchronized void write(K key, V value)
	      throws IOException {

	      boolean nullKey = key == null || key instanceof NullWritable;
	      boolean nullValue = value == null || value instanceof NullWritable;
	      if (nullKey && nullValue) {
	        return;
	      }
	      if (!nullKey) {
	        writeObject(key);
	      }
	      if (!(nullKey || nullValue)) {
	        out.write(recordKVSeparator);
	      }
	      if (!nullValue) {
	        writeObject(value);
	      }
	      out.write(recordSeparator);
	    }

	    public synchronized void close(Reporter reporter) throws IOException {
	      out.close();
	    }
	  }
	  
	@Override
	public RecordWriter<K, V> getRecordWriter(
			FileSystem fs, JobConf job, String name, Progressable progress)
			throws IOException {
	    boolean isCompressed = getCompressOutput(job);
	    String keyValueSeparator = job.get("mapred.textoutputformat.separator", 
	                                       "\t");
	    Path file;
	    
	    if (!isCompressed) {
	        if(fs.getUri().getScheme().equals("file")){
	        	file = new Path(name);
	        }else{
	        	file = FileOutputFormat.getTaskOutputPath(job, name);;
	        }
	        FSDataOutputStream fileOut = fs.create(file, progress);
	      return new GlobalDataRecordWriter<K, V>(fileOut, keyValueSeparator);
	    } else {
	      Class<? extends CompressionCodec> codecClass =
	        getOutputCompressorClass(job, GzipCodec.class);
	      // create the named codec
	      CompressionCodec codec = ReflectionUtils.newInstance(codecClass, job);
	      // build the filename including the extension
	        if(fs.getUri().getScheme().equals("file")){
	        	file = new Path(name + codec.getDefaultExtension());
	        }else{
	        	file = FileOutputFormat.getTaskOutputPath(job, name + codec.getDefaultExtension());
	        }
	        
	      FSDataOutputStream fileOut = fs.create(file, progress);
	      return new GlobalDataRecordWriter<K, V>(new DataOutputStream
	                                        (codec.createOutputStream(fileOut)),
	                                        keyValueSeparator);
	    }
	}

}
