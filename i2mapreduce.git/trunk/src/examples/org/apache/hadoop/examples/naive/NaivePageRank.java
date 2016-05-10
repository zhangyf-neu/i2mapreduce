package org.apache.hadoop.examples.naive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.examples.utils.Util;


public class NaivePageRank {

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
				outputVal.set(tokenList.get(1).getBytes());
				output.collect(outputKey, outputVal);
			} else if(tokenList.size() == 1){
				//no out link
				outputKey.set(tokenList.get(0).getBytes());
				output.collect(outputKey, outputKey);
			}
		}
	}
	
	public static class InitInputReducer extends MapReduceBase
		implements Reducer<Text, Text, LongWritable, Text> {
		Random rand = new Random();
		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {
			StringBuffer outputv = new StringBuffer();
			outputv.append("1:");
			String nodestr = key.toString();
			
			while(values.hasNext()){
				String end = values.next().toString();
				if(end.equals(nodestr)){
					//may be not useful
					int randlong = rand.nextInt(Integer.MAX_VALUE);
					while(randlong == Long.parseLong(end)){
						randlong = rand.nextInt(Integer.MAX_VALUE);
					}
					outputv.append(randlong).append(" ");
				}else{
					outputv.append(end).append(" ");
				}
			}
			
			output.collect(new LongWritable(Long.parseLong(nodestr)), new Text(outputv.toString()));
		}
	}
	
	/**
	 * pagerank mapper and reducer
	 */
	public static class PageRankMapper extends MapReduceBase
		implements Mapper<LongWritable, Text, LongWritable, Text> {

		private int mapcount = 0;
		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {
			mapcount++;
			reporter.setStatus(String.valueOf(mapcount));
			
			long node = Long.parseLong(key.toString());
					
			String valueString = value.toString();
			String[] fields = valueString.split(":");
			float rank = Float.parseFloat(fields[0]);
			
			output.collect(new LongWritable(node), new Text("p:" + fields[1]));

			//output.collect(new LongWritable(node), new Text(String.valueOf(1-PG_DAMPING)));
			
			String[] links = fields[1].split(" ");

			float delta = rank * PG_DAMPING / links.length;
			for(String link : links){
				if(link.equals("") || link.equals("null")) continue;
				output.collect(new LongWritable(Long.parseLong(link)), new Text(String.valueOf(delta)));
			}
		}
	}
	
	public static class PageRankReducer extends MapReduceBase
		implements Reducer<LongWritable, Text, LongWritable, Text> {

		private int redcount = 0;
		
		@Override
		public void reduce(LongWritable key, Iterator<Text> values,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {
			redcount++;
			reporter.setStatus(String.valueOf(redcount));
			
			String links = null;
			float rank = 1 - PG_DAMPING;
			while(values.hasNext()){
				String value = values.next().toString();
				int index = value.indexOf("p:");
				if(index != -1){
					links = value.substring(index+2);
				}else{
					rank += Float.parseFloat(value);
				}		
			}

			StringBuffer out = new StringBuffer();
			output.collect(key, new Text(out.append(rank).append(":").append(links).toString()));		
		}
	}
	
	/**
	 * termination check job
	 */
	
	public static class TermCheckMapper extends MapReduceBase
		implements Mapper<LongWritable, Text, LongWritable, FloatWritable> {

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<LongWritable, FloatWritable> output, Reporter reporter)
				throws IOException {
			String valueString = value.toString();
			String[] fields = valueString.split(":");
			float rank = Float.parseFloat(fields[0]);
			
			output.collect(key, new FloatWritable(rank));
		}
	}
	
	public static class TermCheckReducer extends MapReduceBase
		implements Reducer<LongWritable, FloatWritable, Text, FloatWritable> {
	
		private OutputCollector<Text, FloatWritable> collector;
		private float change = 0;

		@Override
		public void reduce(LongWritable key, Iterator<FloatWritable> values,
				OutputCollector<Text, FloatWritable> output, Reporter reporter)
				throws IOException {
			if(collector == null) collector = output;
			int i = 0;
			float lastvalue = 0;
			while(values.hasNext()){
				i++;
				if(i > 2) System.out.println("something wrong");
				FloatWritable value = values.next();
				if(i == 1){
					lastvalue = value.get();
				}else if(i == 2){
					change += Math.abs(value.get() - lastvalue);
				}
			}
		}
		
		@Override
		public void close() throws IOException{
			collector.collect(new Text("sub change"), new FloatWritable(change));
		}
	}
	
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		if (args.length != 4) {
		      System.err.println("Usage: pagerank <graph> <out dir> <partitions> <maxiteration>");
		      System.exit(2);
		}
		
		String input = args[0];
		String output = args[1];
		int partitions = Integer.parseInt(args[2]);
		int maxiteration = Integer.parseInt(args[3]);
		
		long initstart = System.currentTimeMillis();

		/**
		 * job to init the input data
		 */
		JobConf conf = new JobConf(NaivePageRank.class);
		conf.setJobName("PageRank-Init");
		conf.setMapOutputKeyClass(Text.class);
		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(InitInputMapper.class);
		conf.setReducerClass(InitInputReducer.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output + "/iteration-0"));
		conf.setNumReduceTasks(partitions);
		JobClient.runJob(conf);
		
		long initend = System.currentTimeMillis();
		Util.writeLog("naive.pagerank.log", "init job use " + (initend - initstart)/1000 + " s");
		
		long itertime = 0;
		long totaltime = 0;
		int iteration = 0;
		
		long start = System.currentTimeMillis();
		do {
			iteration++;
			/****************** Main Job ********************************/
			long iterstart = System.currentTimeMillis();;
			conf = new JobConf(NaivePageRank.class);
			conf.setJobName("PageRank-Main");

			conf.setOutputKeyClass(LongWritable.class);
			conf.setOutputValueClass(Text.class);

			conf.setMapperClass(PageRankMapper.class);
			conf.setReducerClass(PageRankReducer.class);
			conf.setInputFormat(SequenceFileInputFormat.class);
			conf.setOutputFormat(SequenceFileOutputFormat.class);

			FileInputFormat.setInputPaths(conf, new Path(output + "/iteration-" + (iteration-1)));
			FileOutputFormat.setOutputPath(conf, new Path(output + "/iteration-" + (iteration)));
			conf.setNumReduceTasks(partitions);

			JobClient.runJob(conf);
			
			long iterend = System.currentTimeMillis();
			itertime += (iterend - iterstart) / 1000;
			
			/******************** Rank Terminate Check Job ***********************/

			conf = new JobConf(NaivePageRank.class);
			conf.setJobName("PageRank-TermCheck");

			conf.setMapOutputKeyClass(LongWritable.class);
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(FloatWritable.class);

			conf.setMapperClass(TermCheckMapper.class);
			conf.setReducerClass(TermCheckReducer.class);

			conf.setInputFormat(SequenceFileInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);

			FileInputFormat.setInputPaths(conf, new Path(output + "/iteration-" + (iteration-1)), new Path(output + "/iteration-" + (iteration)));
			FileOutputFormat.setOutputPath(conf, new Path(output + "/termcheck-" + iteration));
			conf.setNumReduceTasks(partitions);

			JobClient.runJob(conf);
			
			long termend = System.currentTimeMillis();
			totaltime += (termend - iterstart) / 1000;
			
			Util.writeLog("naive.pagerank.log", "iteration computation " + iteration + " takes " + itertime + " s, include termination check takes " + totaltime);
			
		} while (iteration < maxiteration);
    }

}
