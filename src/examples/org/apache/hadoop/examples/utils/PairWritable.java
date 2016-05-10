package org.apache.hadoop.examples.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.examples.SecondarySort.IntPair;
import org.apache.hadoop.io.WritableComparable;

public class PairWritable implements WritableComparable {
	private int x;
	private int y;
	
	public PairWritable(){}
	
	public PairWritable(int a, int b){
		x = a;
		y = b;
	}
	
	public int getX() {return x;}
	public int getY() {return y;}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(x);
		out.writeInt(y);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		x = in.readInt();
		y = in.readInt();
	}

    @Override
    public boolean equals(Object right) {
      if (right instanceof PairWritable) {
    	  PairWritable r = (PairWritable) right;
        return r.getX() == x && r.getY() == y;
      } else {
        return false;
      }
    }
    
	@Override
	public int compareTo(Object o) {
		PairWritable obj = (PairWritable)o;
		if(x == obj.x){
/*			//y<0 means it is a vector index
			if(y < 0 && obj.y < 0){
				return 0;
			}
			//else it is a matrix index
			else{*/
				return (y > obj.y) ? 1 : (y < obj.y) ? -1 : 0;
			//}
		} else{
			return (x > obj.x) ? 1 : -1;
		}
	}
	
	@Override
	public int hashCode(){
		return x*37+y;
	}
	
	@Override
	public String toString(){
		return x + ":" + y;
	}
}
