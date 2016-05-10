package org.apache.hadoop.examples.utils;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class genGraph extends Configured implements Tool {

	public static class genGraphMap extends MapReduceBase
	implements Mapper<LongWritable, Text, LongWritable, Text> {

		private int nummap = 0;
		
		public void configure(JobConf job){
			nummap = job.getNumReduceTasks();
		}
	
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {
			for(int i=0; i<nummap; i++){
				output.collect(new LongWritable(i), value);
			}
		}
	}

	private static void printUsage() {
		System.out.println("disgengraph <in> <outDir> <type>");
		System.out.println(	"\t-p # of parittions\n" +
							"\t-n # of items\n" +
							"\t-arg arguments, depends on type");
	}
	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 3) {
			System.out.println("ERROR: Wrong Input Parameters!");
	        printUsage();
	        return -1;
		}
		
		int partitions = 0;
		int capacity = 0;
		int argument = Integer.MAX_VALUE;
		
		List<String> other_args = new ArrayList<String>();
		for(int i=0; i < args.length; ++i) {
		      try {
		          if ("-p".equals(args[i])) {
		        	partitions = Integer.parseInt(args[++i]);
		          } else if ("-n".equals(args[i])) {
		        	  capacity = Integer.parseInt(args[++i]);
		          } else if ("-arg".equals(args[i])) {
		        	  argument = Integer.parseInt(args[++i]);
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
		      printUsage(); 
		      return -1;
		}
	    
	    String outpath = other_args.get(1);
	    String type = other_args.get(2);
		
	    JobConf job = new JobConf(getConf());
	    job.setJobName("gengraph " + type + ":" + capacity + ":" + argument);    
	    
	    job.setInt(Parameters.GEN_CAPACITY, capacity);
	    job.setInt(Parameters.GEN_ARGUMENT, argument);
	    job.set(Parameters.GEN_TYPE, type);
	    job.set(Parameters.GEN_OUT, outpath);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(outpath));
	    
	    job.setJarByClass(genGraph.class);
	        
	    job.setInputFormat(TextInputFormat.class);
	    job.setOutputFormat(NullOutputFormat.class);
	    
	    job.setMapperClass(genGraphMap.class);
	    job.setReducerClass(genGraphReduce.class);
	    job.setMapOutputKeyClass(LongWritable.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(NullWritable.class);
	    
	    job.setNumReduceTasks(partitions);
	    
	    JobClient.runJob(job);
		return 0;
	}


	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new genGraph(), args);
	    System.exit(res);
	}

}
