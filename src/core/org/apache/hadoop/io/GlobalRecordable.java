package org.apache.hadoop.io;

public interface GlobalRecordable<T> extends Writable{
	String toString();
	void readObject(String line);
}
