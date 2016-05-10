package org.apache.hadoop.examples.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.examples.utils.TrimDimsForKM.TrimDimsMap;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TransposeGraph extends Configured implements Tool{

	public static class TransposeGraphMap extends MapReduceBase
		implements Mapper<LongWritable, Text, IntWritable, Text> {
	
		private Text outputKey = new Text();
		private IntWritable outputVal = new IntWritable();
		private List<String> tokenList = new ArrayList<String>();

		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
			tokenList.clear();

			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

			while (tokenizer.hasMoreTokens()) {
				tokenList.add(tokenizer.nextToken());
			}

			if (tokenList.size() >= 2) {
				outputKey.set(tokenList.get(0).getBytes());
				outputVal.set(Integer.parseInt(tokenList.get(1)));
				output.collect(outputVal, outputKey);
			}
		}
	}
	
	public static class TransposeGraphReduce extends MapReduceBase
		implements Reducer<IntWritable, Text, IntWritable, Text> {
	
		private int node = 0;
		private int total = 0;
		private Random rand = new Random();
		private int offset = 0;

		@Override
		public void configure(JobConf job){
			total = job.getInt("graph.total.node", 0);
			offset = job.getNumReduceTasks();
			node = Util.getTaskId(job);
		}
		
		@Override
		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
			int inputnode = key.get();
			while(inputnode > node){
				int randnode = rand.nextInt(total);
				output.collect(new IntWritable(node), new Text(String.valueOf(randnode)));
				node = node + offset;
			}
			
			while(values.hasNext()){
				Text value = values.next();
				output.collect(key, value);
			}
		}
	}
	
	private static void printUsage() {
		System.out.println("transgraph <input> <output> <total> -p <partition>");
		System.out.println(	"\t-p # of parittions\n");
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		if (args.length < 3) {
			System.out.println("ERROR: Wrong Input Parameters!");
	        printUsage();
	        return -1;
		}
		
		int partitions = 0;
		
		List<String> other_args = new ArrayList<String>();
		for(int i=0; i < args.length; ++i) {
		      try {
		          if ("-p".equals(args[i])) {
		        	partitions = Integer.parseInt(args[++i]);
		          } else {
		    		  other_args.add(args[i]);
		    	  }
		      } catch (NumberFormatException except) {
		        System.out.println("ERROR: Integer expected instead of " + args[i]);
		        printUsage();
		        return -1;
		      } catch (ArrayIndexOutOfBoundsException except) {
		        System.out.println("ERROR: Required parameter missing from " +
		                           args[i-1]);
		        printUsage();
		        return -1;
		      }
		}
		
	    if (other_args.size() < 3) {
		      System.out.println("ERROR: Wrong number of parameters: " +
		                         other_args.size() + ".");
		      printUsage(); return -1;
		}
	    
	    JobConf job = new JobConf(getConf());
	    job.setJobName("transpose graph");  
	    
	    String input = other_args.get(0);
	    String output = other_args.get(1);
	    int total = Integer.parseInt(other_args.get(2));
	    
	    job.setInt("graph.total.node", total);
	    
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    
	    job.setJarByClass(TransposeGraph.class);
	        
	    job.setInputFormat(TextInputFormat.class);
	    job.setOutputFormat(TextOutputFormat.class);
	    
	    job.setMapperClass(TransposeGraphMap.class);
	    job.setReducerClass(TransposeGraphReduce.class);
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setOutputKeyClass(IntWritable.class);
	    
	    job.setNumReduceTasks(partitions);
	    
	    JobClient.runJob(job);
		return 0;
	}
	
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TransposeGraph(), args);
	    System.exit(res);
	}
}
