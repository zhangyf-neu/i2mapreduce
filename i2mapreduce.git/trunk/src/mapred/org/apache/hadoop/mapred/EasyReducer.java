package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Closeable;

public interface EasyReducer<K2, V2, K3, V3> extends JobConfigurable, Closeable {
	  
	 V3 incremental(V3 old,V3 incr);
	 
	 void reduce(K2 key, Iterator<V2> values,
	              OutputCollector<K3, V3> output, Reporter reporter)
	 throws IOException;

	}
