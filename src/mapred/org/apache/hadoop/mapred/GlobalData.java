package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.GlobalUniqValueWritable;
import org.apache.hadoop.io.Writable;
import org.mortbay.log.Log;

public class GlobalData implements Writable{

	GlobalUniqValueWritable instance = new GlobalUniqValueWritable();
	JobID jobID = new JobID();
	int iteration;
	
	public GlobalData(){}
	
	public GlobalData(GlobalUniqValueWritable v, JobID jobid, int iter){
		instance = v;
		jobID = jobid;
		iteration = iter;
	}
	
	public void set(GlobalUniqValueWritable v){
		instance = v;
	}
	
	public GlobalUniqValueWritable get(){
		return instance;
	}
	
	public JobID getJobID(){
		return jobID;
	}
	
	public int getIteration(){
		return iteration;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		jobID.write(out);
		Log.info("write jobid " + jobID);
		out.writeInt(iteration);
		Log.info("write iteration " + iteration);
		//value.write(out);
		Log.info("write value " + instance);
		instance.write(out);	     
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		jobID.readFields(in);
		Log.info("read jobid " + jobID);
		iteration = in.readInt();
		Log.info("read iteration " + iteration);
		//value.readFields(in);

	    instance.readFields(in);
	      
		Log.info("read value " + instance);
	}

}
