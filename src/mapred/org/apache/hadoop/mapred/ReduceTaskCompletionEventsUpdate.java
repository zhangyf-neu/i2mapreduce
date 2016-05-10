package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class ReduceTaskCompletionEventsUpdate implements Writable {
	
	IntWritable[] completeTaskIDs;
	
	public ReduceTaskCompletionEventsUpdate() {};

	public ReduceTaskCompletionEventsUpdate(IntWritable[] taskids) {
		completeTaskIDs = taskids;
	}
	
	public IntWritable[] getCompleteTaskIDs(){
		return completeTaskIDs;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(completeTaskIDs.length);
		for(IntWritable id : completeTaskIDs){
			id.write(out);
		}
	}

	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		completeTaskIDs = new IntWritable[size];
	    for (int i = 0; i < completeTaskIDs.length; i++) {
	    	completeTaskIDs[i] = new IntWritable();
	        completeTaskIDs[i].readFields(in);                          // store it in values
	    }
	}
}
