package org.apache.hadoop.mapred;

public class IndexEntry implements Comparable{
	int hashcode;
	int offset;
	
	public IndexEntry(int hashcode, int offset){
		this.hashcode = hashcode;
		this.offset = offset;
	}

	@Override
	public int compareTo(Object arg0) {
		if(hashcode > ((IndexEntry)arg0).hashcode){
			return 1;
		}else if(hashcode < ((IndexEntry)arg0).hashcode){
			return -1;
		}else{
			if(offset > ((IndexEntry)arg0).offset){
				return 1;
			}else if(offset > ((IndexEntry)arg0).offset){
				return -1;
			}else{
				return 0;
			}
		}
	}
	
	@Override
	public int hashCode(){
		return this.hashcode;
	}
	
	@Override
	public String toString(){
		return new String(String.valueOf(hashcode) + String.valueOf(offset));
	}
}
