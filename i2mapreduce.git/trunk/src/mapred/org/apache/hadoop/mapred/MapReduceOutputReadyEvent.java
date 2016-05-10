package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MapReduceOutputReadyEvent implements Writable {

	boolean mapOutputReady;
	boolean reduceOutputReady;
	boolean globalDataReady;
	boolean depReducerReader;
	
	public MapReduceOutputReadyEvent(){ }
	
	public MapReduceOutputReadyEvent(boolean mapReady, boolean reduceReady){
		this.mapOutputReady = mapReady;
		this.reduceOutputReady = reduceReady;
	}
	
	public boolean isMapOutputReady(){
		return mapOutputReady;
	}
	
	public void setMapOutputReady(boolean ready){
		mapOutputReady = ready;
	}
	
	public boolean isReduceOutputReady(){
		return reduceOutputReady;
	}
	
	public void setReduceOutputReady(boolean ready){
		reduceOutputReady = ready;
	}
	
	public boolean isGlobalDataReady(){
		return globalDataReady;
	}
	
	public void setGlobalDataReady(boolean ready){
		globalDataReady = ready;
	}
	
	public boolean isDepReducerReady(){
		return depReducerReader;
	}
	
	public void setDepReducerReady(boolean ready){
		depReducerReader = ready;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(mapOutputReady);
		out.writeBoolean(reduceOutputReady);
		out.writeBoolean(globalDataReady);
		out.writeBoolean(depReducerReader);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.mapOutputReady = in.readBoolean();
		this.reduceOutputReady = in.readBoolean();
		this.globalDataReady = in.readBoolean();
		this.depReducerReader = in.readBoolean();
	}

}
