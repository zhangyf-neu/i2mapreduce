package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Closeable;

public interface IterativeReducer<K2, V2, K3, V3> extends JobConfigurable, Closeable {

	void reduce(K2 key, Iterator<V2> values,
              OutputCollector<K3, V3> output, Reporter reporter)
            		  throws IOException;
	
	//quantify the difference between the current iteration result and the previous iteration result
	float distance(K3 key, V3 prevV, V3 currV) throws IOException;
	V2 removeLable();
	void iteration_complete(int iteration);
}
