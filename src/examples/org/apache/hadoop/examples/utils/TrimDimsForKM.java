package org.apache.hadoop.examples.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TrimDimsForKM extends Configured implements Tool {

	public static class TrimDimsMap extends MapReduceBase
		implements Mapper<Text, Text, Text, Text> {

		private int dim_num = 0;
		
		public void configure(JobConf job){
			dim_num = job.getInt("km.dimensions.num", 0);
		}
	
		@Override
		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String[] dims_str = value.toString().split(" ");
			
			HashMap<Integer, Integer> dims = new HashMap<Integer, Integer>();
			int total = 0;
			
			for(String dim_str : dims_str){
				String[] item = dim_str.split(",");
				if(item.length != 2) continue;
				int dim_id = Integer.parseInt(item[0]) % dim_num;
				int dim_value = Integer.parseInt(item[1]);
				if(dims.containsKey(dim_id)){
					dims.put(dim_id, dim_value + dims.get(dim_id));
				}else{
					dims.put(dim_id, dim_value);
				}
				
				total += dim_value;
			}
			
			String out_str = "";
			for(Map.Entry<Integer, Integer> dim : dims.entrySet()){
				out_str += dim.getKey() + "," + ((float)dim.getValue()/total) + " ";
			}
			
			output.collect(key, new Text(out_str));
		}
	}

	private static void printUsage() {
		System.out.println("trimdims <input> <output> <dims>");
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
	    job.setJobName("trim dimensions");  
	    
	    String input = other_args.get(0);
	    String output = other_args.get(1);
	    int dims = Integer.parseInt(other_args.get(2));
	    if(partitions == 0){
	    	Util.getTTNum(job);
	    }
	    
	    job.setInt("km.dimensions.num", dims);
	    
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    
	    job.setJarByClass(TrimDimsForKM.class);
	        
	    job.setInputFormat(KeyValueTextInputFormat.class);
	    job.setOutputFormat(TextOutputFormat.class);
	    
	    job.setMapperClass(TrimDimsMap.class);
	    job.setReducerClass(IdentityReducer.class);
	    job.setMapOutputKeyClass(Text.class);
	    	    
	    job.setNumReduceTasks(partitions);
	    
	    JobClient.runJob(job);
		return 0;
	}


	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TrimDimsForKM(), args);
	    System.exit(res);
	}

}
