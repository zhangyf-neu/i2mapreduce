package org.apache.hadoop.examples.iterative;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;



public class StaticDistributeMap extends MapReduceBase implements
		Mapper<Text, Text, IntWritable, Text> {
	@Override
	public void map(Text arg0, Text value,
			OutputCollector<IntWritable, Text> arg2, Reporter arg3)
			throws IOException {
		int page = Integer.parseInt(arg0.toString());

		//normal one			
		arg2.collect(new IntWritable(page), value);
	}

}
