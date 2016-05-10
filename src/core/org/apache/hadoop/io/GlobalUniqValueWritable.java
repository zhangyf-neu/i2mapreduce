package org.apache.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.io.Writable;

public class GlobalUniqValueWritable implements Writable {

	private MapWritable kvs = new MapWritable();
	
	public GlobalUniqValueWritable() {}

	public GlobalUniqValueWritable(MapWritable inkvs) { kvs = inkvs; }

	public MapWritable get(){
		return kvs;
	}
	
	public void set(MapWritable inkvs){
		kvs = (MapWritable)inkvs; 
	}
	
	public int size(){
		return kvs.size();
	}

	public void put(Writable key, Writable value){
		kvs.put(key, value);
	}
	
	public void aggregate(GlobalUniqValueWritable v)
			throws IOException {
		
		for(Map.Entry<Writable, Writable> kv : v.get().entrySet()){
			if(kvs.containsKey(kv.getKey())) throw new IOException("there are two keys " + kv.getKey());
			kvs.put(kv.getKey(), kv.getValue());
		}
		
	}
	
	@Override
	public String toString(){
		String outputstring = "";
		for(Map.Entry<Writable, Writable> entry : kvs.entrySet()){
			outputstring += entry.getKey() + ":" + entry.getValue();
			outputstring += "#";
		}

		return outputstring;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		//positive.write(out);
		kvs.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		//positive.readFields(in);
		kvs.readFields(in);
	}

}

