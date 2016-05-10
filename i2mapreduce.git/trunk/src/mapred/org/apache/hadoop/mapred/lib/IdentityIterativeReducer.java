package org.apache.hadoop.mapred.lib;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.IterativeReducer;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class IdentityIterativeReducer<K, V> extends MapReduceBase implements
		IterativeReducer<K, V, K, V> {

	@Override
	public void reduce(K key, Iterator<V> values,
			OutputCollector<K, V> output, Reporter reporter)
			throws IOException {
		while(values.hasNext()){
			V value = values.next();
			output.collect(key, value);
		}
	}

	@Override
	public float distance(K key, V prevV, V currV) throws IOException{
		return 0;
	}

	@Override
	public V removeLable() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void iteration_complete(int iteration) {
		// TODO Auto-generated method stub
		
	}

}
