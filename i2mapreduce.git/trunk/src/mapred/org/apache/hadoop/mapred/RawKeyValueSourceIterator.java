package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;

public interface RawKeyValueSourceIterator extends RawKeyValueIterator{
	DataInputBuffer getSKey() throws IOException;
}
