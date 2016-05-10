package org.apache.hadoop.examples.incremental;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

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
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Projector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.examples.utils.Parameters;
import org.apache.hadoop.examples.utils.Util;

public class IncrPageRank {

	//damping factor
	public static final float DAMPINGFAC = (float)0.8;
	public static final float RETAINFAC = (float)0.2;
	

	public static class PageRankMap extends MapReduceBase implements
		IterativeMapper<LongWritable, Text, LongWritable, FloatWritable, LongWritable, FloatWritable> {
		
		@Override
		public void map(LongWritable statickey, Text staticval,
				LongWritable dynamickey, FloatWritable dynamicvalue,
				OutputCollector<LongWritable, FloatWritable> output,
				Reporter reporter) throws IOException {
			
			float rank = dynamicvalue.get();
			//System.out.println("input : " + statickey + " : " + rank);
			String linkstring = staticval.toString();
			
			//in order to avoid non-inlink node, which will mismatch the static file
			output.collect(statickey, new FloatWritable(RETAINFAC));
			
			String[] links = linkstring.split(" ");	
			float delta = rank * DAMPINGFAC / links.length;
			
			for(String link : links){
				if(link.equals("")) continue;
				output.collect(new LongWritable(Long.parseLong(link)), new FloatWritable(delta));
				//System.out.println("output: " + link + "\t" + delta);
			}
		}

		@Override
		public FloatWritable removeLable() {
			return new FloatWritable(-1);
		}
	}
	
	public static class PageRankReduce extends MapReduceBase implements
		IterativeReducer<LongWritable, FloatWritable, LongWritable, FloatWritable> {
	
		private long iter_start;
		private long last_iter_end;
		
	    private static int getPid() {
	        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
	        String name = runtime.getName(); // format: "pid@hostname"
	        try {
	            return Integer.parseInt(name.substring(0, name.indexOf('@')));
	        } catch (Exception e) {
	            return -1;
	        }
	    }
	    
		@Override
		public void configure(JobConf job){
			iter_start = job.getLong(Parameters.ITER_START, 0);
			last_iter_end = System.currentTimeMillis();
		}
		
		@Override
		public void reduce(LongWritable key, Iterator<FloatWritable> values,
				OutputCollector<LongWritable, FloatWritable> output, Reporter report)
				throws IOException {
			float rank = 0;
			
			int i = 0;
			while(values.hasNext()){
				float v = values.next().get();
				if(v == -1) continue;	//if the value is equal to the one set by removeLable(), we skip it
				
				i++;
				rank += v;
				
				//System.out.println("reduce on " + key + " with " + v);
			}
			
			//System.out.println(" key " + key + " with " + i);
			
			output.collect(key, new FloatWritable(rank));
			//System.out.println("output\t" + key + "\t" + rank);
		}
		
		@Override
		public float distance(LongWritable key, FloatWritable prevV,
				FloatWritable currV) throws IOException {
			// TODO Auto-generated method stub
			return Math.abs(prevV.get() - currV.get());
		}

		@Override
		public FloatWritable removeLable() {
			// TODO Auto-generated method stub
			return new FloatWritable(-1);
		}

		@Override
		public void iteration_complete(int iteration) {
			long curr_time = System.currentTimeMillis();
			System.out.println("iteration " + iteration + " takes " + 
					(curr_time-last_iter_end) + " total " + (curr_time-iter_start));
			last_iter_end = curr_time;
			
			try{
				String iofile = "/proc/"+getPid()+"/io";
		        Process p = Runtime.getRuntime().exec("cat " + iofile);
		        
		        BufferedReader stdInput = new BufferedReader(new
		             InputStreamReader(p.getInputStream()));
	
		        // read the output from the command
		        System.out.print("iteration " + iteration + " ");
		        String s = "";
		        while ((s = stdInput.readLine()) != null) {
		            System.out.print(s + " ");
		        }
		        System.out.println();
			}catch (IOException e){
				e.printStackTrace();
			}
		}
	}

	public static class PageRankProjector implements Projector<LongWritable, LongWritable, FloatWritable> {

		@Override
		public void configure(JobConf job) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public LongWritable project(LongWritable statickey) {
			return statickey;
		}

		@Override
		public FloatWritable initDynamicV(LongWritable dynamickey) {
			return new FloatWritable(1);
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
		System.out.println("incrpagerank <UpdateStatic> <DeltaStatic> <ConvergedValuePath> <PreservePath> <outDir>");
		System.out.println(	"\t-p # of parittions\n" +
							"\t-t filterthreshold\n" +
							"\t-I # of iterations\n" +
							"\t-c cache type\n" +
							"\t-i checkpoint interval\n" + 
							"\t-T incr support type");
	}
	
	public static int main(String[] args) throws Exception {
		if (args.length < 5) {
			System.out.println("ERROR: Wrong Input Parameters!");
	        printUsage();
	        return -1;
		}
		
	    int partitions = 0;
		double filterthreshold = 0.0;
		int totaliter = 5;
		int cachetype = 6;
		int interval = totaliter;
		int incrtype = 0;
		
	  	  /**
	  	   * cachetype:
	  	   * 0: index only
	  	   * 1: index + fixed single window
	  	   * 2: index + dynamic length single window (according to position)
	  	   * 3: index + fixed multi window (128KB)
	  	   * 4: index + dynamic density-based multi window 1
	  	   * 5: index + dynamic density-based multi window 2
	  	   * 6: index + dynamic interval-based multi window
	  	   * 
	  	   * interval: interval threshold for cachetype 6:
	  	   * default: 100000
	  	   * 1 seek = 10ms, 1s = 100 seeks
	  	   * disk throughput=10MB/s, 1s = 10MB
	  	   * 
	  	   * interval: density for cachetype 4,5
	  	   * e.g., interval=4, means when density>25% window covers them 
	  	   */
		
		List<String> other_args = new ArrayList<String>();
		for(int i=0; i < args.length; ++i) {
		      try {
		          if ("-p".equals(args[i])) {
		        	partitions = Integer.parseInt(args[++i]);
		          } else if ("-t".equals(args[i])) {
		        	  filterthreshold = Double.parseDouble(args[++i]);
		          } else if ("-I".equals(args[i])) {
		        	  totaliter = Integer.parseInt(args[++i]);
		          } else if ("-c".equals(args[i])) {
		        	  cachetype = Integer.parseInt(args[++i]);
		          } else if ("-i".equals(args[i])) {
		        	  interval = Integer.parseInt(args[++i]);
		          } else if ("-T".equals(args[i])) {
		        	  incrtype = Integer.parseInt(args[++i]);
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
		
	    if (other_args.size() < 5) {
		      System.out.println("ERROR: Wrong number of parameters: " +
		                         other_args.size() + ".");
		      printUsage(); return -1;
		}
	    
	    String updateStatic = other_args.get(0);
	    String deltaStatic = other_args.get(1);
	    String convValue = other_args.get(2);
	    String preserveState = other_args.get(3);
	    String output = other_args.get(4);
	    

		String iteration_id = "incrpagerank" + new Date().getTime();
 
	    /**
	     * Incremental start job, which is the first job of the incremental jobs
	     */
    	long incrstart = System.currentTimeMillis();
    	
	    JobConf incrstartjob = new JobConf(IncrPageRank.class);
	    String jobname = "Incr PageRank Start" + new Date().getTime();
	    incrstartjob.setJobName(jobname);

	    //set for iterative process   
	    incrstartjob.setIncrementalStart(true);
	    incrstartjob.setIterativeAlgorithmID(iteration_id);		//must be unique for an iterative algorithm
	    
	    incrstartjob.setDeltaUpdatePath(deltaStatic);				//the out dated static data
	    incrstartjob.setPreserveStatePath(preserveState);		// the preserve map/reduce output path
	    incrstartjob.setConvergeStatePath(convValue);				// the stable dynamic data path
	    //incrstartjob.setDynamicDataPath(convValue);				// the stable dynamic data path
	    incrstartjob.setIncrOutputPath(output);
	    
	    incrstartjob.setStaticInputFormat(SequenceFileInputFormat.class);
	    incrstartjob.setDynamicInputFormat(SequenceFileInputFormat.class);		//MUST have this for the following jobs, even though the first job not need it
	    incrstartjob.setResultInputFormat(SequenceFileInputFormat.class);		//if set termination check, you have to set this
	    incrstartjob.setOutputFormat(SequenceFileOutputFormat.class);
	    
	    incrstartjob.setStaticKeyClass(LongWritable.class);
	    incrstartjob.setStaticValueClass(Text.class);
	    incrstartjob.setOutputKeyClass(LongWritable.class);
	    incrstartjob.setOutputValueClass(FloatWritable.class);
	    
	    FileInputFormat.addInputPath(incrstartjob, new Path(deltaStatic));
	    FileOutputFormat.setOutputPath(incrstartjob, new Path(output + "/" + iteration_id + "/iteration-0"));	//the filtered output dynamic data

	    incrstartjob.setFilterThreshold((float)filterthreshold);

	    incrstartjob.setIterativeMapperClass(PageRankMap.class);	
	    incrstartjob.setIterativeReducerClass(PageRankReduce.class);
	    incrstartjob.setProjectorClass(PageRankProjector.class);
	    
	    incrstartjob.setPreserveBufferType(cachetype);
	    incrstartjob.setPreserveBufferInterval(interval);
	    
	    incrstartjob.setNumMapTasks(partitions);
	    incrstartjob.setNumReduceTasks(partitions);			

	    JobClient.runJob(incrstartjob);
	    
    	long incrend = System.currentTimeMillis();
    	long incrtime = (incrend - incrstart) / 1000;
    	Util.writeLog("incr.pagerank.log", "incremental start computation takes " + incrtime + " s");
	    
    	/**
    	 * the iterative incremental jobs
    	 */
	    long itertime = 0;
	    
    	long iterstart = System.currentTimeMillis();
    	
	    JobConf incriterjob = new JobConf(IncrPageRank.class);
	    jobname = "Incr PageRank Iterative Computation " + iterstart;
	    incriterjob.setJobName(jobname);
	    incriterjob.setLong(Parameters.ITER_START, iterstart);
	    
	    //set for iterative process   

		//incremental support type 0: MRBG+CPC
		//incremental support type 1: MRBG only
		//incremental support type 2: CPC only
	    if(incrtype == 0){
	    	incriterjob.setIncrementalIterative(true);
	    }else if(incrtype == 1){
	    	incriterjob.setIncrMRBGOnly(true);
	    }else if(incrtype == 2){
	    	incriterjob.setIterCPC(true);
	    }
	    
	    incriterjob.setIterativeAlgorithmID(iteration_id);		//must be unique for an iterative algorithm
	    incriterjob.setMaxIterations(totaliter);					//max number of iterations

	    incriterjob.setStaticDataPath(updateStatic);				//the new static data
	    incriterjob.setPreserveStatePath(preserveState);		// the preserve map/reduce output path
	    incriterjob.setDynamicDataPath(output + "/" + iteration_id);				// the dynamic data path
	    incriterjob.setIncrOutputPath(output);
	    
	    incriterjob.setStaticInputFormat(SequenceFileInputFormat.class);
	    incriterjob.setDynamicInputFormat(SequenceFileInputFormat.class);		//MUST have this for the following jobs, even though the first job not need it
	    incriterjob.setResultInputFormat(SequenceFileInputFormat.class);		//if set termination check, you have to set this
    	incriterjob.setOutputFormat(SequenceFileOutputFormat.class);
	    
    	incriterjob.setStaticKeyClass(LongWritable.class);
    	incriterjob.setStaticValueClass(Text.class);
    	incriterjob.setOutputKeyClass(LongWritable.class);
    	incriterjob.setOutputValueClass(FloatWritable.class);
	    
	    FileInputFormat.addInputPath(incriterjob, new Path(updateStatic));
	    FileOutputFormat.setOutputPath(incriterjob, new Path(output + "/" + iteration_id + "/iter")); 	//the filtered output dynamic data

	    incriterjob.setFilterThreshold((float)filterthreshold);
	    incriterjob.setBufferReduceKVs(true);
	    incriterjob.setPreserveBufferType(cachetype);
	    incriterjob.setPreserveBufferInterval(interval);
	    
	    
	    incriterjob.setIterativeMapperClass(PageRankMap.class);	
	    incriterjob.setIterativeReducerClass(PageRankReduce.class);
	    incriterjob.setProjectorClass(PageRankProjector.class);
	    
	    incriterjob.setNumMapTasks(partitions);
	    incriterjob.setNumReduceTasks(partitions);	
	    
	    
	    JobClient.runIterativeJob(incriterjob);

    	long iterend = System.currentTimeMillis();
    	itertime += (iterend - iterstart) / 1000;
    	Util.writeLog("incr.pagerank.log", "cachetype "+ cachetype + " iteration computation takes " + itertime + " s");
    	
	    
	    return 0;
	}
}
