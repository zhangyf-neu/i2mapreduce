package org.apache.hadoop.examples.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FormatGraph extends Configured implements Tool{

	public static class TransposeGraphMap extends MapReduceBase
		implements Mapper<LongWritable, Text, LongWritable, LongWritable> {
	
		private LongWritable outputKey = new LongWritable();
		private LongWritable outputVal = new LongWritable();
		private List<String> tokenList = new ArrayList<String>();

		public void map(LongWritable key, Text value,
				OutputCollector<LongWritable, LongWritable> output, Reporter reporter)
				throws IOException {
			tokenList.clear();

			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

			while (tokenizer.hasMoreTokens()) {
				tokenList.add(tokenizer.nextToken());
			}

			if (tokenList.size() >= 2) {
				outputKey.set(Long.parseLong(tokenList.get(0)));
				outputVal.set(Long.parseLong(tokenList.get(1)));
				output.collect(outputKey, outputVal);
			}
		}
	}

	public static class TransposeGraphReduce extends MapReduceBase
		implements Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

		@Override
		public void reduce(LongWritable key, Iterator<LongWritable> values,
				OutputCollector<LongWritable, LongWritable> output, Reporter reporter)
				throws IOException {
			while(values.hasNext()){
				LongWritable value = values.next();
				output.collect(key, value);
			}
		}
	}
	
	private static void printUsage() {
		System.out.println("transgraph <input> <output> -p <partition>");
		System.out.println(	"\t-p # of parittions\n");
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		if (args.length < 2) {
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
		
	    if (other_args.size() < 2) {
		      System.out.println("ERROR: Wrong number of parameters: " +
		                         other_args.size() + ".");
		      printUsage(); return -1;
		}
	    
	    JobConf job = new JobConf(getConf());
	    job.setJobName("format graph");  
	    
	    String input = other_args.get(0);
	    String output = other_args.get(1);
	    
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    
	    job.setJarByClass(FormatGraph.class);
	        
	    job.setInputFormat(TextInputFormat.class);
	    job.setOutputFormat(TextOutputFormat.class);
	    
	    job.setMapperClass(TransposeGraphMap.class);
	    job.setReducerClass(TransposeGraphReduce.class);
	    job.setMapOutputKeyClass(LongWritable.class);
	    job.setMapOutputValueClass(LongWritable.class);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(LongWritable.class);
	    
	    job.setNumReduceTasks(partitions);
	    
	    JobClient.runJob(job);
		return 0;
	}
	
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new FormatGraph(), args);
	    System.exit(res);
	}
}

