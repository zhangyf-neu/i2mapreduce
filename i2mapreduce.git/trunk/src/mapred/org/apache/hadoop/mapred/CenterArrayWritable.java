package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.GlobalRecordable;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;

public class CenterArrayWritable implements GlobalRecordable  { 
	

	  @Override
	public String toString() {
		String outstring = "";
		for (DoubleWritable  element : center) {
			outstring += element.get()+",";
		}
		return outstring;
	}

	private Class<? extends Writable> valueClass;
	  private DoubleWritable[] center; 
	  
	  public CenterArrayWritable(){
		  
	  }
	  
	  public CenterArrayWritable(DoubleWritable []  incenter){
		  center=new DoubleWritable[incenter.length];
		  center = incenter; 
	  }
	  public CenterArrayWritable(double []  incenter){
		  center=new DoubleWritable[incenter.length];
		  for (int i = 0; i < incenter.length; i++) {
			  center[i]=new DoubleWritable(incenter[i]);
		}
	 }
  
    public void set(DoubleWritable[] center) { this.center = center; }

	  public Writable[] get() { return center; }
	  public double[] get(Class<? extends Writable> valueClass) { 
		  double d[]=new double[center.length];
		  for (int i = 0; i < center.length; i++) {
				d[i]=((DoubleWritable)center[i]).get();
		  } 
		  return d; 
	  }

	  public void readFields(DataInput in) throws IOException {
		  center = new DoubleWritable[in.readInt()];          // construct values
	    for (int i = 0; i < center.length; i++) {
	      DoubleWritable value = new DoubleWritable();
	      value.readFields(in);                       // read a value
	      center[i] = value;                          // store it in values
	    }
	  }

	  public void write(DataOutput out) throws IOException {
	    out.writeInt(center.length);                 // write values
	    for (int i = 0; i < center.length; i++) {
	    	center[i].write(out);
	    }
	  }

	  @Override
	  public void readObject(String line) { 
		try {
			StringTokenizer tokenizer = new StringTokenizer(line, ",");
			int size = tokenizer.countTokens();
			 
			center = new DoubleWritable[size];
			int i = 0;
			while (tokenizer.hasMoreTokens()) {
				String attribute = tokenizer.nextToken();
				center[i] = new DoubleWritable(Double.parseDouble(attribute));
				i++;
			}
	 
		} catch (Exception e) {
			StringTokenizer tokenizer = new StringTokenizer(line, " ");
			int size = tokenizer.countTokens();
	
			center = new DoubleWritable[size];
			int i = 0;
			while (tokenizer.hasMoreTokens()) {
				String attribute = tokenizer.nextToken();
				center[i] = new DoubleWritable(Double.parseDouble(attribute));
				i++;
			}
		}

//
//	
//		private double [] center;
//		public CenterArrayWritable(){}
//		
//		public CenterArrayWritable(double []  incenter){
//			center = incenter;
//		}
//		
//		public double []  get(){
//			return center;
//		}
	
//	@Override
//	public void write(DataOutput out) throws IOException {
//		out.writeInt(center.length);
////		for(Map.Entry<Integer, Double> field : center.entrySet()){
////			out.writeInt(field.getKey());
////			out.writeDouble(field.getValue());
////		}
//		for (double  element : center) {
//			out.writeDouble(element);
//		}
//	}

//	@Override
//	public void readFields(DataInput in) throws IOException {
//		int fieldsize = in.readInt();
//		for(int j=0; j<fieldsize; j++){
////			center.put(in.readInt(), in.readDouble());
//			center[j]=in.readDouble();
//		}
//	}
	
//	@Override
//	public String toString(){
//		String outstring = "";
//		for (double  element : center) {
//			outstring += element+",";
//		}
////		for(Map.Entry<Integer, Double> field : center.entrySet()){
////			outstring += field.getKey() + "," + field.getValue() + " ";
////		}
//		return outstring;
//	}

//	@Override
//	public void readObject(String line) { 
//
//		try {
//			StringTokenizer tokenizer = new StringTokenizer(line, ",");
//			int size = tokenizer.countTokens();
//	
//			center = new double[size];
//			int i = 0;
//			while (tokenizer.hasMoreTokens()) {
//				String attribute = tokenizer.nextToken();
//				center[i] = Double.parseDouble(attribute);
//				i++;
//			}
//	 
//		} catch (Exception e) {
//			StringTokenizer tokenizer = new StringTokenizer(line, " ");
//			int size = tokenizer.countTokens();
//	
//			center = new double[size];
//			int i = 0;
//			while (tokenizer.hasMoreTokens()) {
//				String attribute = tokenizer.nextToken();
//				center[i] = Double.parseDouble(attribute);
//				i++;
//			} 
//		}
	
//		StringTokenizer st = new StringTokenizer(line);
//		while(st.hasMoreTokens()){
//			String field = st.nextToken();
//			String[] field2 = field.split(",");
//			center.put(Integer.parseInt(field2[0]), Double.parseDouble(field2[1]));
//		}
	}
}
