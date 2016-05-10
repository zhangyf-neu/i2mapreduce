package org.apache.hadoop.examples.iterative;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class StateDistributeMap extends MapReduceBase implements
		Mapper<Text, Text, IntWritable, FloatWritable> {
	
	@Override
	public void map(Text key, Text value,
			OutputCollector<IntWritable, FloatWritable> output, Reporter arg3)
			throws IOException {
		int page = Integer.parseInt(key.toString());
		float score = Float.parseFloat(value.toString());
		output.collect(new IntWritable(page), new FloatWritable(score));
	}

}
