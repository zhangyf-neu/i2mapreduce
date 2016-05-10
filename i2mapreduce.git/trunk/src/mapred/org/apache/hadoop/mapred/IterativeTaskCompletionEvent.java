package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class IterativeTaskCompletionEvent implements Writable {

	JobID jobID = new JobID();
	int iterationNum = 0;
	TaskAttemptID attemptID = new TaskAttemptID();
	int taskid = 0;
	boolean ismap = true;
	long processedRecords = 0;
	long spentMilliseconds = 0;
	float distance = 0;

	
	public IterativeTaskCompletionEvent() {};
	
	public IterativeTaskCompletionEvent(JobID job, int iterNum, 
			TaskAttemptID attemptID, int id, boolean isMap){
		jobID = job;
		iterationNum = iterNum;
		this.attemptID = attemptID;
		taskid = id;
		ismap = isMap;
	}
	
	public int getIteration(){
		return iterationNum;
	}
	
	public TaskAttemptID getAttemptID(){
		return attemptID;
	}
	
	public void setAttemptID(TaskAttemptID attemptID){
		this.attemptID = attemptID;
	}
	
	public int gettaskID(){
		return taskid;
	}
	
	public boolean getIsMap(){
		return ismap;
	}
	
	public float getSubDistance(){
		return distance;
	}
	
	public void setSubDistance(float subdistance){
		distance = subdistance;
	}
	
	public long getProcessedRecords(){
		return processedRecords;
	}
	
	public void setProcessedRecords(long records){
		processedRecords = records;
	}
	
	public void setRunTime(long milliseconds){
		spentMilliseconds = milliseconds;
	}
	
	public long getRunTime(){
		return spentMilliseconds;
	}
	

	
	public JobID getJob(){
		return jobID;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.jobID.readFields(in);
		this.iterationNum = in.readInt();
		this.attemptID.readFields(in);
		this.taskid = in.readInt();
		this.ismap = in.readBoolean();
		this.processedRecords = in.readLong();
		this.spentMilliseconds = in.readLong();
		this.distance = in.readFloat();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.jobID.write(out);
		out.writeInt(this.iterationNum);
		this.attemptID.write(out);
		out.writeInt(this.taskid);
		out.writeBoolean(this.ismap);
		out.writeLong(this.processedRecords);
		out.writeLong(this.spentMilliseconds);
		out.writeFloat(this.distance);
	}
	
	@Override
	public String toString(){
		return "IterativeTaskCompletionEvent --- jobid: " + jobID +
				"\niteration: " + iterationNum +
				"\nattempttaskid: " + attemptID + 
				"\nisMap: " + ismap +
				"\nprocessedRecords: " + processedRecords +
				"\nspentMilliseconds: " + spentMilliseconds + 
				"\ndistance: " + distance;
	
	}
}
