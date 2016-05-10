package org.apache.hadoop.examples.naive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import jsc.distributions.Lognormal;

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

public class NaiveSSSP {
	/**
	 * initialize input data maper and reducer
	 */
	public static class InitInputMapper extends MapReduceBase
		implements Mapper<LongWritable, Text, Text, Text> {
		
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
	
		private int source = 0;
		Lognormal logn = new Lognormal(0, 1);
		
		@Override
		public void configure(JobConf job){
			source = job.getInt("sssp.source.id", 0);
		}
		
		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {
			StringBuffer outputv = new StringBuffer();
			String nodestr = key.toString();
			if(Integer.parseInt(nodestr) == source){
				outputv.append("0:");
			}else{
				outputv.append(Float.MAX_VALUE).append(":");
			}
			
			while(values.hasNext()){
				String end = values.next().toString();
				float weight = (float)Math.abs(logn.random());
				outputv.append(end).append(",").append(weight).append(" ");
			}
			
			output.collect(new LongWritable(Long.parseLong(nodestr)), new Text(outputv.toString()));
		}
	}
	
	/**
	 * SSSP mapper and reducer
	 */
	public static class SSSPMapper extends MapReduceBase
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
			float length = Float.parseFloat(fields[0]);
			
			output.collect(new LongWritable(node), new Text("p:" + fields[1]));
			
			output.collect(new LongWritable(node), new Text(String.valueOf(length)));
			
			String[] links = fields[1].split(" ");

			for(String link : links){
				if(link.equals("") || link.equals("null")) continue;
				
				long end = Long.parseLong(link.substring(0, link.indexOf(",")));
				float weight = Float.parseFloat(link.substring(link.indexOf(",") + 1));
				
				output.collect(new LongWritable(end), new Text(String.valueOf(length + weight)));
			}
		}
	}
	
	public static class SSSPReducer extends MapReduceBase
		implements Reducer<LongWritable, Text, LongWritable, Text> {

		private int redcount = 0;
		
		@Override
		public void reduce(LongWritable key, Iterator<Text> values,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {
			redcount++;
			reporter.setStatus(String.valueOf(redcount));
			
			String links = null;
			float length = Float.MAX_VALUE;
			
			while(values.hasNext()){
				String value = values.next().toString();
				int index = value.indexOf("p:");
				if(index != -1){
					links = value.substring(index+2);
				}else{
					length = Math.min(length, Float.parseFloat(value));
				}		
			}

			StringBuffer out = new StringBuffer();
			output.collect(key, new Text(out.append(length).append(":").append(links).toString()));		
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
		private int stable = 0;

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
					if(value.get() == lastvalue){
						stable++;
					}
				}
			}
		}
		
		@Override
		public void close() throws IOException{
			collector.collect(new Text("sub change"), new FloatWritable(stable));
		}
	}
	
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		if (args.length != 5 ) {
		      System.err.println("Usage: sssp <graph> <out dir> <partitions> <maxiteration> <source>");
		      System.exit(2);
		}
		
		String input = args[0];
		String output = args[1];
		int partitions = Integer.parseInt(args[2]);
		int maxiteration = Integer.parseInt(args[3]);
		int source = Integer.parseInt(args[4]);
		
		long initstart = System.currentTimeMillis();

		/**
		 * job to init the input data
		 */
		JobConf conf = new JobConf(NaiveSSSP.class);
		conf.setJobName("SSSP-Init");
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
		conf.setInt("sssp.source.id", source);
		JobClient.runJob(conf);
		
		long initend = System.currentTimeMillis();
		Util.writeLog("naive.sssp.log", "init job use " + (initend - initstart)/1000 + " s");
		
		long itertime = 0;
		long totaltime = 0;
		int iteration = 0;
		
		do {
			iteration++;
			/****************** Main Job ********************************/
			long iterstart = System.currentTimeMillis();;
			conf = new JobConf(NaiveSSSP.class);
			conf.setJobName("SSSP-Main");

			conf.setOutputKeyClass(LongWritable.class);
			conf.setOutputValueClass(Text.class);

			conf.setMapperClass(SSSPMapper.class);
			conf.setReducerClass(SSSPReducer.class);
			conf.setInputFormat(SequenceFileInputFormat.class);
			conf.setOutputFormat(SequenceFileOutputFormat.class);

			FileInputFormat.setInputPaths(conf, new Path(output + "/iteration-" + (iteration-1)));
			FileOutputFormat.setOutputPath(conf, new Path(output + "/iteration-" + (iteration)));
			conf.setNumReduceTasks(partitions);

			JobClient.runJob(conf);
			
			long iterend = System.currentTimeMillis();
			itertime += (iterend - iterstart) / 1000;
			
			/******************** Rank Terminate Check Job ***********************/

			conf = new JobConf(NaiveSSSP.class);
			conf.setJobName("SSSP-TermCheck");

			conf.setMapOutputKeyClass(LongWritable.class);
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(FloatWritable.class);

			conf.setMapperClass(TermCheckMapper.class);
			conf.setReducerClass(TermCheckReducer.class);

			conf.setInputFormat(SequenceFileInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);

			FileInputFormat.setInputPaths(conf, new Path(output + "/iteration-" + (iteration-1)), new Path(output + "/iteration-" + (iteration)));
			FileOutputFormat.setOutputPath(conf, new Path(output + "/termcheck-" + iteration));
			conf.setNumReduceTasks(1);

			JobClient.runJob(conf);
			
			long termend = System.currentTimeMillis();
			totaltime += (termend - iterstart) / 1000;
			
			Util.writeLog("naive.sssp.log", "iteration computation " + iteration + " takes " + itertime + " s, include termination check takes " + totaltime);
			
		} while (iteration < maxiteration);
    }
}
