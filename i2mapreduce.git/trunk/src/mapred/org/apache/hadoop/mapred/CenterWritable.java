package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.io.GlobalRecordable;

public class CenterWritable implements GlobalRecordable<TreeMap<Integer, Double>> {
	private TreeMap<Integer, Double> center = new TreeMap<Integer, Double>();
	
	public CenterWritable(){}
	
	public CenterWritable(TreeMap<Integer, Double> incenter){
		center = incenter;
	}
	
	public TreeMap<Integer, Double> get(){
		return center;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(center.size());
		for(Map.Entry<Integer, Double> field : center.entrySet()){
			out.writeInt(field.getKey());
			out.writeDouble(field.getValue());
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int fieldsize = in.readInt();
		for(int j=0; j<fieldsize; j++){
			center.put(in.readInt(), in.readDouble());
		}
	}
	
	@Override
	public String toString(){
		String outstring = "";
		for(Map.Entry<Integer, Double> field : center.entrySet()){
			outstring += field.getKey() + "," + field.getValue() + " ";
		}
		return outstring;
	}

	@Override
	public void readObject(String line) {
		StringTokenizer st = new StringTokenizer(line);
		while(st.hasMoreTokens()){
			String field = st.nextToken();
			String[] field2 = field.split(",");
			center.put(Integer.parseInt(field2[0]), Double.parseDouble(field2[1]));
		}
	}
}
