package org.apache.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GlobalUniqKeyWritable implements WritableComparable {

	private int globalkey = 0;
	
	public GlobalUniqKeyWritable() {}

	  
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(globalkey);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		globalkey = in.readInt();
	}

	@Override
	public int compareTo(Object o) {
		return 0;
	}

	@Override
	public boolean equals(Object o){
		return true;
	}
	
	@Override
	public String toString(){
		return new String("GlobalKey");
	}
}
