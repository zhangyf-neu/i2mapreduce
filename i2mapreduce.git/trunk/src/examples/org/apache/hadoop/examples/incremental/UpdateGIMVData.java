package org.apache.hadoop.examples.incremental;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.examples.utils.Util;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;

public class UpdateGIMVData {

	public static class UpdateDataMap extends MapReduceBase implements
		Mapper<LongWritable, Text, NullWritable, NullWritable> {
		private Random rand = new Random();
		private double changepercent;
		private BufferedWriter update_writer;
		private BufferedWriter delta_writer;
		private int capacity;
		
		@Override
		public void configure(JobConf job){
			changepercent = job.getFloat("incr.gimv.change.percent", -1);
			capacity = job.getInt("gimv.data.capacity", -1);
	
			FileSystem fs;
			try {
				fs = FileSystem.get(job);
				Path deltapath = new Path(job.get("gimv.delta.update.path") + "/part-" + Util.getTaskId(job));
				delta_writer = new BufferedWriter(new OutputStreamWriter(fs.create(deltapath)));
				
				Path updatepath = new Path(job.get("gimv.update.update.path") + "/part-" + Util.getTaskId(job));
				update_writer = new BufferedWriter(new OutputStreamWriter(fs.create(updatepath)));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<NullWritable, NullWritable> output, Reporter reporter)
				throws IOException {
			String inputv = value.toString();
			
			String outputv = "";
			if(rand.nextDouble() < changepercent) {
				//update
				String[] item = inputv.split(" ");
				
				float newvalue = rand.nextFloat();
				outputv = item[0] + " " + item[1] + " " + newvalue;
				
				//write to the delta file
				delta_writer.write(outputv + " +\n");
				System.out.println("delta:\t" + outputv + " +");
				
				update_writer.write(outputv + "\n");
				System.out.println("output\t" + outputv);
			}else{
				//also write to the delta file
				delta_writer.write(inputv + "\n");
				System.out.println("delta\t" + inputv);
				
				update_writer.write(inputv + "\n");
				System.out.println("output\t" + inputv);
			}
		}
		
		@Override
		public void close() throws IOException{
			delta_writer.close();
			update_writer.close();
		}
	}
	
	private static void printUsage() {
		System.out.println("gengimvupdate <OldStatic> <UpdateDataPath> <DeltaDataPath>");
		System.out.println(	"\t-p # of parittions\n" +
							"\t-u update percent\n" +
							"\t-n # of rows/columns of squared matrix\n");
	}
	
	public static int main(String[] args) throws Exception {
		if (args.length < 3) {
			System.out.println("ERROR: Wrong Input Parameters!");
	        printUsage();
	        return -1;
		}
		
		int partitions = -1;
		int capacity = 100;
		float update_percent = 0;
		
		List<String> other_args = new ArrayList<String>();
		for(int i=0; i < args.length; ++i) {
		      try {
		    	  if ("-p".equals(args[i])) {
		        	  partitions = Integer.parseInt(args[++i]);
		          } else if ("-n".equals(args[i])) {
		        	  capacity = Integer.parseInt(args[++i]);
		          } else if ("-u".equals(args[i])) {
		        	  update_percent = Float.parseFloat(args[++i]);
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
	    
	    String input = other_args.get(0);
	    String update_output = other_args.get(1);
	    String delta_output = other_args.get(2);
	    
	    JobConf job0 = new JobConf(UpdateGIMVData.class);
	    
	    if(partitions == -1){
	    	partitions = Util.getTTNum(job0);
	    }
	    
		/**
		 * update the graph manually
		 */
	    
	    String jobname0 = "GIM-V Update Generation";
	    job0.setJobName(jobname0);
	    
	    job0.setOutputKeyClass(NullWritable.class);
	    job0.setOutputValueClass(NullWritable.class);
	    job0.setMapperClass(UpdateDataMap.class);
	    job0.setReducerClass(IdentityReducer.class);
	    job0.setInputFormat(TextInputFormat.class);
	    job0.setOutputFormat(TextOutputFormat.class);
	    
	    FileInputFormat.addInputPath(job0, new Path(input));
	    FileOutputFormat.setOutputPath(job0, new Path("/tmp" + System.currentTimeMillis()));
	
	    job0.setFloat("incr.gimv.change.percent", update_percent);		//the delta change percent of update/add
	    job0.set("gimv.delta.update.path", delta_output);
	    job0.set("gimv.update.update.path", update_output);
	    job0.setInt("gimv.data.capacity", capacity);
	
	    job0.setNumReduceTasks(partitions);
	    
	    JobClient.runJob(job0);
		
		return 0;
	}
}
