package org.apache.hadoop.mapred;
//package org.apache.hadoop.mapred.lib;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.EasyReducer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class IdentityEasyReducer<K2, V2, K3, V3>
extends MapReduceBase implements EasyReducer<K2, V2, K3, V3>{

	@Override
	public V3 incremental(V3 old, V3 incr) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void reduce(K2 key, Iterator<V2> values,
			OutputCollector<K3, V3> output, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub
		
	}

}
