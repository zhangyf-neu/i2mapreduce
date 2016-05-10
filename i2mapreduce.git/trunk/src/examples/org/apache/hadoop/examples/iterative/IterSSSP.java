package org.apache.hadoop.examples.iterative;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.examples.utils.Parameters;
import org.apache.hadoop.examples.utils.Util;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.IterativeMapper;
import org.apache.hadoop.mapred.IterativeReducer;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Projector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.HashPartitioner;

public class IterSSSP {
	
	public static class DistributeDataMap extends MapReduceBase implements
		Mapper<Text, Text, LongWritable, Text> {
		
		@Override
		public void map(Text arg0, Text value,
				OutputCollector<LongWritable, Text> arg2, Reporter arg3)
				throws IOException {
			long page = Long.parseLong(arg0.toString());
		
			arg2.collect(new LongWritable(page), value);
		}
	}
	
	public static class DistributeDataReduce extends MapReduceBase implements
		Reducer<LongWritable, Text, LongWritable, Text> {
		
		@Override
		public void reduce(LongWritable key, Iterator<Text> values,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {
			String outv = "";
			while(values.hasNext()){
				String v = values.next().toString();
				outv = v.substring(v.indexOf(":") + 1);
			}
			
			output.collect(key, new Text(outv));
		}
	}
	
	public static class DistributeDataMap2 extends MapReduceBase implements
		Mapper<LongWritable, Text, LongWritable, Text> {
		
		@Override
		public void map(LongWritable arg0, Text value,
				OutputCollector<LongWritable, Text> arg2, Reporter arg3)
				throws IOException {
			arg2.collect(arg0, value);
		}
	}

	public static class DistributeDataMap3 extends MapReduceBase implements
		Mapper<LongWritable, Text, LongWritable, Text> {
		
		private LongWritable outputKey = new LongWritable();
		private Text outputVal = new Text();
		private List<String> tokenList = new ArrayList<String>();

		public void map(LongWritable key, Text value,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {
			tokenList.clear();

			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

			while (tokenizer.hasMoreTokens()) {
				tokenList.add(tokenizer.nextToken());
			}

			if (tokenList.size() >= 2) {
				outputKey.set(Integer.parseInt(tokenList.get(0)));
				outputVal.set(tokenList.get(1).getBytes());
				output.collect(outputKey, outputVal);
			} else if(tokenList.size() == 1){
				//no out link
				System.out.println("node " + key + " has no out links");
			}
		}
	}
	
	public static class SSSPMap extends MapReduceBase implements
		IterativeMapper<LongWritable, Text, LongWritable, FloatWritable, LongWritable, FloatWritable> {
	
		@Override
		public void map(LongWritable statickey, Text staticval,
				LongWritable dynamickey, FloatWritable dynamicvalue,
				OutputCollector<LongWritable, FloatWritable> output,
				Reporter reporter) throws IOException {

/*			if(statickey.get() == 204367){
				System.out.println("map 204367");
				if(System.currentTimeMillis() % 4 == 0){
					System.out.println("error");
					int[] c = new int[1];
					c[1] = 22;
				}
			}*/
			
			float length = dynamicvalue.get();
			output.collect(statickey, dynamicvalue);
			
			//System.out.println("input : " + dynamickey + " : " + rank);
			String linkstring = staticval.toString();
			
			String[] links = linkstring.split(" ");
			for(String link : links){
				if(link.equals("")) continue;
				
				long end = Long.parseLong(link.substring(0, link.indexOf(",")));
				float weight = Float.parseFloat(link.substring(link.indexOf(",") + 1));
				
				output.collect(new LongWritable(end), new FloatWritable(length + weight));
				//System.out.println("output: " + link + "\t" + delta);
			}
		}

		@Override
		public FloatWritable removeLable() {
			// TODO Auto-generated method stub
			return null;
		}
	}
	
	public static class SSSPReduce extends MapReduceBase implements
		IterativeReducer<LongWritable, FloatWritable, LongWritable, FloatWritable> {
	
		private long iter_start;
		private long last_iter_end;
		
		@Override
		public void configure(JobConf job){
			iter_start = job.getLong(Parameters.ITER_START, 0);
			last_iter_end = iter_start;
		}
		
		@Override
		public void reduce(LongWritable key, Iterator<FloatWritable> values,
				OutputCollector<LongWritable, FloatWritable> output, Reporter report)
				throws IOException {

			
			float length = Float.MAX_VALUE;
			while(values.hasNext()){
				float v = values.next().get();
				//if(v == -1) continue;
				length = Math.min(length, v);
				//System.out.println("reduce on " + key + " with " + v);
			}
			
			output.collect(key, new FloatWritable(length));
			
			
/*			if(key.get() == 204367){
				System.out.println("dividing 204367");
				if(System.currentTimeMillis() % 4 == 0){
					System.out.println("error");
					int[] c = new int[1];
					c[1] = 22;
				}
			}*/
			
			//System.out.println(key + "\t" + rank);
		}
		
		@Override
		public float distance(LongWritable key, FloatWritable prevV,
				FloatWritable currV) throws IOException {
			if(prevV.get() != currV.get()){
				return 1;
			}else{
				return 0;
			}
		}

		@Override
		public FloatWritable removeLable() {
			return null;
		}

		@Override
		public void iteration_complete(int iteration) {
			long curr_time = System.currentTimeMillis();
			System.out.println("iteration " + iteration + " takes " + 
					(curr_time-last_iter_end) + " total " + (curr_time-iter_start));
		}
	}
	
	public static class SSSPProjector implements Projector<LongWritable, LongWritable, FloatWritable> {

		private int source;
		
		@Override
		public void configure(JobConf job) {
			source = job.getInt("sssp.source.id", 0);
		}

		@Override
		public LongWritable project(LongWritable statickey) {
			return statickey;
		}

		@Override
		public FloatWritable initDynamicV(LongWritable dynamickey) {
			if(dynamickey.get() == source){
				return new FloatWritable(0);
			}else{
				return new FloatWritable(Float.MAX_VALUE);
			}
		}

		@Override
		public Partitioner<LongWritable, FloatWritable> getDynamicKeyPartitioner() {
			// TODO Auto-generated method stub
			return new HashPartitioner<LongWritable, FloatWritable>();
		}

		@Override
		public org.apache.hadoop.mapred.Projector.Type getProjectType() {
			return Projector.Type.ONE2ONE;
		}
	}

	private static void printUsage() {
		System.out.println("sssp [-p partitions] <inStaticDir> <outDir> <source>");
		System.out.println(	"\t-p # of parittions\n" +
							"\t-i snapshot interval\n" +
							"\t-I # of iterations\n" +
							"\t-D initial dynamic path\n" +
							"\t-f input format\n" + 
							"\t-s run preserve job" +
							"\t-t preserve type");
	}

	public static int main(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("ERROR: Wrong Input Parameters!");
	        printUsage();
	        return -1;
		}
		
		int partitions = 0;
		int interval = 1;
		int max_iterations = Integer.MAX_VALUE;
		String init_dynamic = "";
		String data_format = "";
		boolean preserve = true;
		
		List<String> other_args = new ArrayList<String>();
		for(int i=0; i < args.length; ++i) {
		      try {
		          if ("-p".equals(args[i])) {
		        	partitions = Integer.parseInt(args[++i]);
		          } else if ("-i".equals(args[i])) {
		        	interval = Integer.parseInt(args[++i]);
		          } else if ("-I".equals(args[i])) {
		        	  max_iterations = Integer.parseInt(args[++i]);
		          } else if ("-D".equals(args[i])) {
		        	  init_dynamic = args[++i];
		          } else if ("-f".equals(args[i])) {
		        	  data_format = args[++i];
		          } else if ("-s".equals(args[i])) {
		        	  preserve = Boolean.parseBoolean(args[++i]);
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
	    
	    String inStatic = other_args.get(0);
	    String output = other_args.get(1);
	    int source = Integer.parseInt(other_args.get(2));
		
	    String iteration_id = "SSSP" + new Date().getTime();
	    
	    /*
		/**
		 * the initialization job, for partition the data and workload
		 */
	    long initstart = System.currentTimeMillis();
	    
	    JobConf job1 = new JobConf(IterSSSP.class);
	    String jobname1 = "SSSP Init";
	    job1.setJobName(jobname1);
	    
	    job1.setDataDistribution(true);
	    job1.setIterativeAlgorithmID(iteration_id);
	    
	    if(data_format.equals("KeyValueTextInputFormat")){
	    	job1.setInputFormat(KeyValueTextInputFormat.class);
	    	job1.setMapperClass(DistributeDataMap.class);
	    	job1.setReducerClass(DistributeDataReduce.class);
	    }else if(data_format.equals("SequenceFileInputFormat")){
	    	job1.setInputFormat(SequenceFileInputFormat.class);
	    	job1.setMapperClass(DistributeDataMap2.class);
	    	job1.setReducerClass(DistributeDataReduce.class);
	    }else if(data_format.equals("TextInputFormat")){
	    	job1.setInputFormat(TextInputFormat.class);
	    	job1.setMapperClass(DistributeDataMap3.class);
	    	job1.setReducerClass(DistributeDataReduce.class);
	    }
	    
	    job1.setOutputFormat(SequenceFileOutputFormat.class);
	    FileInputFormat.addInputPath(job1, new Path(inStatic));
	    FileOutputFormat.setOutputPath(job1, new Path(output + "/substatic"));

	    job1.setOutputKeyClass(LongWritable.class);
	    job1.setOutputValueClass(Text.class);
	    
	    //new added, the order is strict
	    job1.setProjectorClass(SSSPProjector.class);
	    
	    /**
	     * if partitions to0 small, which limit the map performance (since map is usually more haveyly loaded),
	     * we should partition the static data into 2*partitions, and copy reduce results to the other mappers in the same scale,
	     * buf first, we just consider the simple case, static data partitions == dynamic data partitions
	     */
	    job1.setNumMapTasks(partitions);
	    job1.setNumReduceTasks(partitions);
	    
	    JobClient.runJob(job1);
	    
	    long initend = System.currentTimeMillis();
		Util.writeLog("iter.sssp.log", "init job use " + (initend - initstart)/1000 + " s");
		
	    /**
	     * start iterative application jobs
	     */
	    long itertime = 0;
	    
	    //while(cont && iteration < max_iterations){
    	long iterstart = System.currentTimeMillis();
    	
	    JobConf job = new JobConf(IterSSSP.class);
	    String jobname = "Iter SSSP ";
	    job.setJobName(jobname);
    
	    //if(partitions == 0) partitions = Util.getTTNum(job);
	    
	    //set for iterative process   
	    job.setIterative(true);
	    job.setIterativeAlgorithmID(iteration_id);		//must be unique for an iterative algorithm
	    job.setLong(Parameters.ITER_START, iterstart);

	    if(max_iterations == Integer.MAX_VALUE){
	    	job.setDistanceThreshold(1);
	    }else{
	    	job.setMaxIterations(max_iterations);
	    }
	    
	    if(init_dynamic == ""){
	    	job.setInitWithFileOrApp(false);
	    }else{
	    	job.setInitWithFileOrApp(true);
	    	job.setInitStatePath(init_dynamic);
	    }
	    job.setStaticDataPath(output + "/substatic");
	    job.setDynamicDataPath(output + "/result");	
	    
	    job.setStaticInputFormat(SequenceFileInputFormat.class);
	    job.setDynamicInputFormat(SequenceFileInputFormat.class);		//MUST have this for the following jobs, even though the first job not need it
    	job.setResultInputFormat(SequenceFileInputFormat.class);		//if set termination check, you have to set this
	    job.setOutputFormat(SequenceFileOutputFormat.class);
	    
	    FileInputFormat.addInputPath(job, new Path(output + "/substatic"));
	    FileOutputFormat.setOutputPath(job, new Path(output + "/result"));
	    
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(FloatWritable.class);
	    
	    job.setIterativeMapperClass(SSSPMap.class);	
	    job.setIterativeReducerClass(SSSPReduce.class);
	    job.setProjectorClass(SSSPProjector.class);
	    job.setInt("sssp.source.id", source);
	    
	    job.setNumReduceTasks(partitions);			
	    JobClient.runIterativeJob(job);

    	long iterend = System.currentTimeMillis();
    	itertime += (iterend - iterstart) / 1000;
    	Util.writeLog("iter.sssp.log", "iteration computation takes " + itertime + " s");
	    	
    	
	    if(preserve){
		    //preserving job
	    	long preservestart = System.currentTimeMillis();
	    	
		    JobConf job2 = new JobConf(IterSSSP.class);
		    jobname = "SSSP Preserve ";
		    job2.setJobName(jobname);
	    
		    if(partitions == 0) partitions = Util.getTTNum(job2);
		    
		    //set for iterative process   
		    job2.setPreserve(true);
		    job2.setIterativeAlgorithmID(iteration_id);		//must be unique for an iterative algorithm
		    //job2.setIterationNum(iteration);					//iteration numbe
		    job2.setCheckPointInterval(interval);					//checkpoint interval
		    job2.setStaticDataPath(output + "/substatic");
		    job2.setDynamicDataPath(output + "/result/iteration-" + max_iterations);	
		    job2.setStaticInputFormat(SequenceFileInputFormat.class);
		    job2.setDynamicInputFormat(SequenceFileInputFormat.class);		//MUST have this for the following jobs, even though the first job not need it
	    	job2.setResultInputFormat(SequenceFileInputFormat.class);		//if set termination check, you have to set this
		    job2.setOutputFormat(SequenceFileOutputFormat.class);
		    job2.setPreserveStatePath(output + "/preserve");
		    
		    FileInputFormat.addInputPath(job2, new Path(output + "/substatic"));
		    FileOutputFormat.setOutputPath(job2, new Path(output + "/preserve/convergeState"));
		    
		    if(max_iterations == Integer.MAX_VALUE){
		    	job2.setDistanceThreshold(1);
		    }

		    job2.setStaticKeyClass(LongWritable.class);
		    job2.setOutputKeyClass(LongWritable.class);
		    job2.setOutputValueClass(FloatWritable.class);
		    
		    job2.setIterativeMapperClass(SSSPMap.class);	
		    job2.setIterativeReducerClass(SSSPReduce.class);
		    job2.setProjectorClass(SSSPProjector.class);
		    
		    job2.setNumReduceTasks(partitions);			

		    JobClient.runIterativeJob(job2);

	    	long preserveend = System.currentTimeMillis();
	    	long preservationtime = (preserveend - preservestart) / 1000;
	    	Util.writeLog("iter.sssp.log", "iteration preservation takes " + preservationtime + " s");
	    }
		return 0;
	}
}
