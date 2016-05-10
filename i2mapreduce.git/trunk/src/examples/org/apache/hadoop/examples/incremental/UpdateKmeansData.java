package org.apache.hadoop.examples.incremental;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.IFile;
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
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.examples.utils.Util;

public class UpdateKmeansData {

	public static final int DEFAULT_DIM_NUM = 10;
	public static final int DEFAULT_VALUE_RANGE = 100;
	
	public static class UpdateDataMap extends MapReduceBase implements
		Mapper<Text, Text, IntWritable, Text> {

		@Override
		public void map(Text key, Text value,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
			output.collect(new IntWritable(Integer.parseInt(key.toString())), value);
		}
		
	}
	
	public static class UpdateDataReduce extends MapReduceBase implements
		Reducer<IntWritable, Text, IntWritable, Text> {
		
		private Random rand = new Random();
		private OutputCollector<IntWritable, Text> collector = null;
		private int lastkey;
		private double change_percent;
		private double delete_percent;
		private double add_percent;
		
		private int possible_dims;
		private int dimvalue_range;
		private int total_keys;
		private IFile.TrippleWriter<IntWritable, Text, Text> writer;
		private JobConf conf;
		
		@Override
		public void configure(JobConf job){
			conf = job;
			change_percent = job.getFloat("incr.kmeans.update.percent", -1);
			delete_percent = job.getFloat("incr.kmeans.delete.percent", -1);
			add_percent = job.getFloat("incr.kmeans.add.percent", -1);
			possible_dims = job.getInt("incr.kmeans.dim.num", -1);
			dimvalue_range = job.getInt("incr.kmeans.dimvalue.range", -1);
			
			FileSystem fs;
			try {
				fs = FileSystem.get(job);
				Path deltapath = new Path(job.get("kmeans.delta.update.path") + "/part-" + Util.getTaskId(job));
				writer = new IFile.TrippleWriter<IntWritable, Text, Text>(job, fs, deltapath, 
						IntWritable.class, Text.class, Text.class, null, null);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		@Override
		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
			if(collector == null){
				collector = output;
			}
			
			String outputv = "";
			while(values.hasNext()){
				outputv = values.next().toString();
			}
			
			//System.out.println("input: " + key + "\t" + outputv);
			
			//randomlly change
			if(rand.nextDouble() < change_percent ){
				String newpoint = new String(); 
				String[] itemstrs = outputv.split(" ");
				
				int count = 0;
				for(String itemstr : itemstrs){
					if(count++ == 0) continue;		//leave at least one dimension
					
					//update each dim, add or sub a random value
					String[] item = itemstr.split(",");
					String dim_id = item[0];
					int dim_value = Integer.parseInt(item[1]);
					int new_dim_value = 0;
					if(rand.nextBoolean() == true){
						new_dim_value = dim_value + rand.nextInt(dimvalue_range);
						
						newpoint += dim_id + "," + new_dim_value;
					}else{
						new_dim_value = dim_value - rand.nextInt(dimvalue_range);
						if(new_dim_value > 0){
							newpoint += dim_id + "," + new_dim_value;
						}
					}
				}
				output.collect(key, new Text(newpoint));
				writer.append(key, new Text(outputv), new Text("-"));
				writer.append(key, new Text(newpoint), new Text("+"));
			}else if(rand.nextDouble() < delete_percent){
				//don't output, only write to delta input
				writer.append(key, new Text(outputv), new Text("-"));
			}else{
				output.collect(key, new Text(outputv));
			}
			
			lastkey = key.get();
			total_keys++;
		}
		
		@Override
		public void close() throws IOException{
			int addpoints_num = (int) (total_keys * add_percent);
			for(int i=0; i<addpoints_num; i++){
				int add_key = lastkey + conf.getNumReduceTasks();
				
				//add a new point
				int num_dim = rand.nextInt(possible_dims) + 1;
				Set<Integer> choosed_dims = new HashSet<Integer>(num_dim);
				
				String point = new String(); 
				for(int j=0; j<num_dim; j++){
					int dim_id = rand.nextInt(possible_dims) + 1;
					
					while(choosed_dims.contains(dim_id)){
						dim_id = rand.nextInt(possible_dims) + 1;
					}
					choosed_dims.add(dim_id);
					
					int dim_value = rand.nextInt(dimvalue_range) + 1;
					point += dim_id + "," + dim_value + " ";
				}
				
				collector.collect(new IntWritable(add_key), new Text(point));
				writer.append(new IntWritable(add_key), new Text(point), new Text("+"));
				
				//System.out.println(added + "\t" + outputv + "\t+");
			}
			
			writer.close();
		}
	}

	private static void printUsage() {
		System.out.println("genkmupdate <OldStatic> <UpdateDataPath> <DeltaDataPath>");
		System.out.println(	"\t-p # of parittions\n" +
							"\t-n # of possible dimentions\n" +
							"\t-v dim_value range\n" +
							"\t-u update percent\n" +
							"\t-d delete percent\n" +
							"\t-a add percent\n");
	}
	
	public static int main(String[] args) throws Exception {
		if (args.length < 3) {
			System.out.println("ERROR: Wrong Input Parameters!");
	        printUsage();
	        return -1;
		}
		
		int partitions = -1;
		int possible_dims = -1;
		int value_range = -1;
		float update_percent = 0;
		float delete_percent = 0;
		float add_percent = 0;
		
		List<String> other_args = new ArrayList<String>();
		for(int i=0; i < args.length; ++i) {
		      try {
		          if ("-p".equals(args[i])) {
		        	partitions = Integer.parseInt(args[++i]);
		          } else if ("-n".equals(args[i])) {
		        	  possible_dims = Integer.parseInt(args[++i]);
		          } else if ("-v".equals(args[i])) {
		        	  value_range = Integer.parseInt(args[++i]);
		          } else if ("-u".equals(args[i])) {
		        	  update_percent = Float.parseFloat(args[++i]);
		          } else if ("-d".equals(args[i])) {
		        	  delete_percent = Float.parseFloat(args[++i]);
		          } else if ("-a".equals(args[i])) {
		        	  add_percent = Float.parseFloat(args[++i]);
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
	    
	    JobConf job0 = new JobConf(UpdateKmeansData.class);
	    
	    if(partitions == -1){
	    	partitions = Util.getTTNum(job0);
	    }
	    
	    if(possible_dims == -1){
	    	possible_dims = DEFAULT_DIM_NUM;
	    }
	    
	    if(value_range == -1){
	    	value_range = DEFAULT_VALUE_RANGE;
	    }
	    
	    long initstart = System.currentTimeMillis();
	    
	    String jobname0 = "Kmeans Update Generation";
	    job0.setJobName(jobname0);
	    
	    job0.setInputFormat(KeyValueTextInputFormat.class);
	    job0.setOutputFormat(TextOutputFormat.class);
	    FileInputFormat.addInputPath(job0, new Path(input));
	    FileOutputFormat.setOutputPath(job0, new Path(update_output));
	
	    job0.setMapperClass(UpdateDataMap.class);
	    job0.setReducerClass(UpdateDataReduce.class);
	
	    job0.setMapOutputKeyClass(IntWritable.class);
	    job0.setMapOutputValueClass(Text.class);
	    
	    job0.setFloat("incr.kmeans.update.percent", update_percent);		//the delta change percent of update/add
	    job0.setFloat("incr.kmeans.delete.percent", delete_percent);
	    job0.setFloat("incr.kmeans.add.percent", add_percent);
	    job0.set("kmeans.delta.update.path", delta_output);
	    job0.setInt("incr.kmeans.dim.num", possible_dims);
	    job0.setInt("incr.kmeans.dimvalue.range", value_range);
	
	    job0.setNumReduceTasks(partitions);
	    
	    JobClient.runJob(job0);
	    
	    long initend = System.currentTimeMillis();
		//Util.writeLog("incr.pagerank.log", "update job use " + (initend - initstart)/1000 + " s");
		
		return 0;
	}
}
