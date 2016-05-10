package org.apache.hadoop.examples.naive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.examples.naive.NaivePageRank.InitInputMapper;
import org.apache.hadoop.examples.naive.NaivePageRank.InitInputReducer;
import org.apache.hadoop.examples.naive.NaivePageRank.PageRankMapper;
import org.apache.hadoop.examples.naive.NaivePageRank.PageRankReducer;
import org.apache.hadoop.examples.naive.NaivePageRank.TermCheckMapper;
import org.apache.hadoop.examples.naive.NaivePageRank.TermCheckReducer;
import org.apache.hadoop.examples.utils.Util;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class Test {
	public static final float PG_DAMPING = (float)0.8;
	/**
	 * initialize input data maper and reducer
	 */
	public static class InitInputMapper extends MapReduceBase
		implements Mapper<LongWritable, Text, Text, Text> {
/*
		private String startValue = "1";
		
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			int index = line.indexOf("\t");
			if(index == -1){
				System.out.println("input no tab");
			}
			String keytext = line.substring(0, index);
			String valuetext = startValue + ":" + line.substring(index+1);
			
			output.collect(new Text(keytext), new Text(valuetext));
		}
*/
		private Text outputKey = new Text();
		private Text outputVal = new Text();
		private List<String> tokenList = new ArrayList<String>();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			tokenList.clear();

			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

			while (tokenizer.hasMoreTokens()) {
				tokenList.add(tokenizer.nextToken());
			}

			if (tokenList.size() >= 2) {
				outputKey.set(tokenList.get(0).getBytes());
				
				outputVal.set("hahaha " + tokenList.get(1));
				output.collect(outputKey, outputVal);
			}
		}
	}


	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
		      System.err.println("Usage: test <graph> <out dir>");
		      System.exit(2);
		}
		
		String input = args[0];
		String output = args[1];
		
		long initstart = System.currentTimeMillis();

		JobConf conf = new JobConf(Test.class);
		conf.setJobName("test");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(InitInputMapper.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));
		conf.setNumReduceTasks(0);
		JobClient.runJob(conf);
		
    }
}
