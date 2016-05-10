package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.GlobalRecordable;
import org.apache.hadoop.io.GlobalUniqKeyWritable;
import org.apache.hadoop.io.GlobalUniqValueWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;

public class GlobalDataInputFormat extends FileInputFormat<GlobalUniqKeyWritable, GlobalUniqValueWritable> implements
		JobConfigurable {
	
	private JobConf job;
	
	class GlobalRecordReader implements RecordReader<GlobalUniqKeyWritable, GlobalUniqValueWritable> {
		  private final LineRecordReader lineRecordReader;

		  private byte separator = (byte) '\t';

		  private LongWritable dummyKey;

		  private Text innerValue;
		  
		  public Class getKeyClass() { return GlobalUniqKeyWritable.class; }
		  
		  public GlobalUniqKeyWritable createKey() {
		    return new GlobalUniqKeyWritable();
		  }
		  
		  public GlobalUniqValueWritable createValue() {
		    return new GlobalUniqValueWritable();
		  }

		  public GlobalRecordReader(Configuration job, FileSplit split)
		    throws IOException {
			  if(split.getIsLocal()){
				  lineRecordReader = new LineRecordReader(job, split, true);
			  }else{
				  lineRecordReader = new LineRecordReader(job, split, false);	
			  }
		    
		    dummyKey = lineRecordReader.createKey();
		    innerValue = lineRecordReader.createValue();
		    String sepStr = job.get("key.value.separator.in.input.line", "\t");
		    this.separator = (byte) sepStr.charAt(0);
		  }
		  
		  public int findSeparator(byte[] utf, int start, int length, byte sep) {
		    for (int i = start; i < (start + length); i++) {
		      if (utf[i] == sep) {
		        return i;
		      }
		    }
		    return -1;
		  }

		  /** Read key/value pair in a line. */
		  public synchronized boolean next(GlobalUniqKeyWritable key, GlobalUniqValueWritable value)
		    throws IOException {
			  MapWritable centers = new MapWritable();
			  
			  GlobalUniqKeyWritable tKey = key;
			  GlobalUniqValueWritable tValue = value;
		    byte[] line = null;
		    do{
		    	int lineLen = -1;
			    if (lineRecordReader.next(dummyKey, innerValue)) {
			      line = innerValue.getBytes();
			      lineLen = innerValue.getLength();
			    } else {
			      return false;
			    }
			    
			    int pos = findSeparator(line, 0, lineLen, this.separator);
			    if (pos == -1) {
			    	throw new IOException("position is -1");
			    } else {
			      int keyLen = pos;
			      
			      int valLen = lineLen - keyLen - 1;
			      byte[] valBytes = new byte[valLen];
			      System.arraycopy(line, pos + 1, valBytes, 0, valLen);
			      String centers_str = new String(valBytes);
			      
			      StringTokenizer st = new StringTokenizer(centers_str, "#");
			      while(st.hasMoreTokens()){
			    	  String center_str = st.nextToken();
			    	  //System.out.println(center_str);
			    	  
			    	  int index = center_str.indexOf(":");
			    	  int center_id = Integer.parseInt(center_str.substring(0, index));
			    	  String center_datastr = center_str.substring(index + 1);
			    	  
			    	  GlobalRecordable record = (GlobalRecordable) ReflectionUtils.newInstance(job.getOutputValueClass(), job);
			    	  record.readObject(center_datastr);
				      centers.put(new IntWritable(center_id), record);
			      }

			      tValue.set(centers);
			    }
		    }while(line == null);
		    
		    return true;
		  }
		  
		  public float getProgress() {
		    return lineRecordReader.getProgress();
		  }
		  
		  public synchronized long getPos() throws IOException {
		    return lineRecordReader.getPos();
		  }

		  public synchronized void close() throws IOException { 
		    lineRecordReader.close();
		  }
	}
	

	@Override
	public void configure(JobConf job) {
		this.job = job;
	}
	
	@Override
	public RecordReader<GlobalUniqKeyWritable, GlobalUniqValueWritable> getRecordReader(InputSplit split, JobConf job, 
			Reporter reporter) throws IOException {
	    return new GlobalRecordReader(job, (FileSplit) split);
	}
}
