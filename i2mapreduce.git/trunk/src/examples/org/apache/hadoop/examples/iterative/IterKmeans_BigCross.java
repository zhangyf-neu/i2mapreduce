package org.apache.hadoop.examples.iterative;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map; 
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.utils.Parameters;
import org.apache.hadoop.examples.utils.Util;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.GlobalUniqKeyWritable;
import org.apache.hadoop.io.GlobalUniqValueWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.CenterArrayWritable; 
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.GlobalDataInputFormat;
import org.apache.hadoop.mapred.GlobalDataOutputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.IterativeMapper;
import org.apache.hadoop.mapred.IterativeReducer;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable; 
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Projector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapred.lib.IntTextKVInputFormat;
import org.mortbay.log.Log;
 

public class IterKmeans_BigCross {

	public static class IntCenterKVInputFormat extends FileInputFormat<IntWritable, CenterArrayWritable> implements
	JobConfigurable {

		private CompressionCodecFactory compressionCodecs = null;
		
		protected boolean isSplitable(FileSystem fs, Path file) {
		    return compressionCodecs.getCodec(file) == null;
		}
		  
		@Override
		public void configure(JobConf job) {
			// TODO Auto-generated method stub
			compressionCodecs = new CompressionCodecFactory(job);
		}
		
		@Override
		public RecordReader<IntWritable, CenterArrayWritable> getRecordReader(InputSplit split, JobConf job,
				Reporter reporter) throws IOException {
		    reporter.setStatus(split.toString());
		    return new IntCenterKVLineRecordReader(job, (FileSplit) split);
		}
	
	}
	
	public static class IntCenterKVLineRecordReader implements RecordReader<IntWritable, CenterArrayWritable> {
		  private final LineRecordReader lineRecordReader;

		  private byte separator = (byte) '\t';

		  private LongWritable dummyKey;

		  private Text innerValue;
		  
		  public Class getKeyClass() { return IntWritable.class; }
		  
		  public IntWritable createKey() {
		    return new IntWritable();
		  }
		  
		  public CenterArrayWritable createValue() {
		    return new CenterArrayWritable();
		  }

		  public IntCenterKVLineRecordReader(Configuration job, FileSplit split)
		    throws IOException {
		    lineRecordReader = new LineRecordReader(job, split, true);
		    dummyKey = lineRecordReader.createKey();
		    innerValue = lineRecordReader.createValue();
		    String sepStr = job.get("key.value.separator.in.input.line", "\t");
		    this.separator = (byte) sepStr.charAt(0);
		  }

		  public IntCenterKVLineRecordReader(Configuration job, InputStream in)
				    throws IOException {
				    
		    lineRecordReader = new LineRecordReader(in, 0, Integer.MAX_VALUE, job);
		    dummyKey = lineRecordReader.createKey();
		    innerValue = lineRecordReader.createValue();
		    String sepStr = job.get("key.value.separator.in.input.line", "\t");
		    this.separator = (byte) sepStr.charAt(0);
		  }
		  
		  public static int findSeparator(byte[] utf, int start, int length, byte sep) {
		    for (int i = start; i < (start + length); i++) {
		      if (utf[i] == sep) {
		        return i;
		      }
		    }
		    return -1;
		  }

		  /** Read key/value pair in a line. */
		  public synchronized boolean next(IntWritable key, CenterArrayWritable value)
		    throws IOException {
			IntWritable tKey = key;
			CenterArrayWritable tValue = value;
		    byte[] line = null;
		    int lineLen = -1;
		    if (lineRecordReader.next(dummyKey, innerValue)) {
		      line = innerValue.getBytes();
		      lineLen = innerValue.getLength();
		    } else {
		      return false;
		    }
		    if (line == null)
		      return false;
		    int pos = findSeparator(line, 0, lineLen, this.separator);
		    if (pos == -1) {
		    	Log.info("position is -1");
		    	
		      tKey.set(Integer.MAX_VALUE);
		    } else {
		      int keyLen = pos;
		      byte[] keyBytes = new byte[keyLen];
		      System.arraycopy(line, 0, keyBytes, 0, keyLen);
		      int keyvalue = Integer.parseInt(new String(keyBytes));
		      int valLen = lineLen - keyLen - 1;
		      byte[] valBytes = new byte[valLen];
		      System.arraycopy(line, pos + 1, valBytes, 0, valLen);
		      tKey.set(keyvalue);
		      tValue.readObject(new String(valBytes));
		    }
		    return true;
		  }
		  
		  public float getProgress() {
		    return lineRecordReader.getProgress();
		  }
		  
		  public synchronized long getPos() throws IOException {
		    return lineRecordReader.getPos();
		  }

		  public synchronized void close() throws IOException { 
		    lineRecordReader.close();
		  }
	}

	
	
	public static class DistributeDataMap extends MapReduceBase 
	 	implements Mapper<LongWritable, Text, IntWritable, Text> {
	
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
			//System.out.println(key + "\t" + value);
			String line = value.toString();
			int pos = line.indexOf(",");
			output.collect(new IntWritable(Integer.parseInt(line.substring(0, pos))), 
							new Text(line.substring(pos + 1)));
		}
	}
	
	public static class DistributeDataReduce extends MapReduceBase
		implements Reducer<IntWritable, Text, IntWritable, Text>{
		private JobConf conf;
		private String initCenterDir;
		private GlobalUniqValueWritable initCenters = new GlobalUniqValueWritable();
		private int k;
		private int count = 0;
		private HashMap<Integer, String> out = new HashMap<Integer, String>();
		private Random rand = new Random();
		
		@Override
		public void configure(JobConf job){
			conf = job;
			k = job.getInt("kmeans.cluster.k", 0);
			initCenterDir = job.getInitStatePath();
		}
		
		@Override
		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<IntWritable, Text> output, Reporter report) throws IOException {
  
			Text value = new Text();
			while(values.hasNext()) {
				value = values.next();
				output.collect(key, value);	
			}
			
			
			//randomly collect initial centers, only one reducer collect
			if(Util.getTaskId(conf) == 0){
				if(count < k){
					String[] items = value.toString().split(",");
					DoubleWritable[] center=new DoubleWritable[items.length];
					
					for (int i = 0; i < items.length; i++) {
						center[i]=new DoubleWritable(Double.parseDouble(items[i]));
					}
					initCenters.put(new IntWritable(count), new CenterArrayWritable(center));
					
					System.out.println("put " + count + "\t" + value.toString());
					count++;
				}else{
					int r = rand.nextInt(count);
					if(r < k){
						String[] items = value.toString().split(",");
						DoubleWritable[] center=new DoubleWritable[items.length];
						
						for (int i = 0; i < items.length; i++) {
							center[i]=new DoubleWritable(Double.parseDouble(items[i]));
						}
						initCenters.put(new IntWritable(r), new CenterArrayWritable(center));
						
						System.out.println("put " + r + "\t" + value.toString());
					}
					count++;
				}
			}
		}
	
		@Override
		public void close() {
			if(Util.getTaskId(conf) != 0) return;
			FileSystem fs;
			try {
				fs = FileSystem.get(conf);
				Path initCenterPath = new Path(initCenterDir);		
				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(initCenterPath)));
		
				System.out.println(initCenters.size());
				
				bw.write(new GlobalUniqKeyWritable() + "\t" + initCenters);
				bw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	
	
	public static class KmeansMap extends MapReduceBase implements
		IterativeMapper<IntWritable, Text, GlobalUniqKeyWritable, GlobalUniqValueWritable, IntWritable, Text> {
	
		private Map<Integer, double[]> centers = null;
		private int count = 0;
		
		private double distance(double[] first, double[] second){
			double euclidean = 0;
			int len=(first.length>second.length)?second.length:first.length;
			 
			for (int i = 0; i <len; i++) {
				euclidean+=(first[i]-second[i])*(first[i]-second[i]);
			}
			
			return Math.sqrt(euclidean);
		}
		
		@Override
		public void map(IntWritable statickey, Text staticval,
				GlobalUniqKeyWritable dynamickey,
				GlobalUniqValueWritable dynamicvalue,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
			
			if(centers == null){
				centers = new HashMap<Integer, double[]>();
				MapWritable rawcenters = dynamicvalue.get();
				
//				System.out.println("read centers " + dynamicvalue.toString());
				
				for(Map.Entry<Writable, Writable> entry : rawcenters.entrySet()){
					int centerid = ((IntWritable)entry.getKey()).get();
					double[] centerdata = ((CenterArrayWritable)entry.getValue()).get(DoubleWritable.class);
					centers.put(centerid, centerdata); 
				}
			}
			
			count++;
			reporter.setStatus(String.valueOf(count));
 
			String line[]=staticval.toString().split(",");
			double [] item_dims=new double[line.length];
			for (int i = 0; i < line.length; i++) {
				item_dims[i]=Double.parseDouble(line[i]);
			}
			
			double min_distance = Double.MAX_VALUE;
			int simcluster = -1;
			for(Map.Entry<Integer, double[]> mean : centers.entrySet()){
				int centerid = mean.getKey();
				double[] centerdata = mean.getValue();
				double distance = distance(centerdata, item_dims);
				
				if(distance < min_distance) {
					min_distance = distance;
					simcluster = centerid;
				}
			}

			if(simcluster == -1){
				System.out.println("simcluster is -1 " + statickey + "\t" + simcluster + "\tvalue " + staticval);
			}
			output.collect(new IntWritable(simcluster), staticval); 
		}

		@Override
		public Text removeLable() {
			// TODO Auto-generated method stub
			return null;
		}
	}
	
	public static class KmeansReduce extends MapReduceBase implements
		IterativeReducer<IntWritable, Text, IntWritable, CenterArrayWritable> {
	
		private long iter_start;
		private long last_iter_end;
		
		@Override
		public void configure(JobConf job){
			iter_start = job.getLong(Parameters.ITER_START, 0);
			last_iter_end = iter_start;
		}
		
		@Override
		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<IntWritable, CenterArrayWritable> output, Reporter report) throws IOException {
			
			//input key: cluster's mean  (whose mean has the nearest measure distance)
			//input value: user-id data

			int num=0;
			double[] all_items=null;
			if(values.hasNext()){
				String line[]=values.next().toString().split(",");
				all_items=new double[line.length];
				for (int i = 0; i < line.length; i++) {
					all_items[i]=Double.parseDouble(line[i]);
				}
				num++;
			}
			
			while(values.hasNext()){
				String line[]=values.next().toString().split(",");
				for (int i = 0; i < line.length; i++) {
					all_items[i]+=Double.parseDouble(line[i]);
				}
				num++;
			}
 
			double avg[]=new double[all_items.length];
			for (int i = 0; i < all_items.length; i++) {
				avg[i]=all_items[i]/num; 
			} 
			output.collect(new IntWritable(key.get()), new CenterArrayWritable(avg));
			
//			System.out.print("output " + key + "\t" );
//			for (int i = 0; i < all_items.length; i++) {
//				System.out.print(avg[i] );
//			}
//			System.out.println();
		}

		@Override
		public float distance(IntWritable key, CenterArrayWritable prevV,
				CenterArrayWritable currV) throws IOException {
			float distance = 0;
			
			double [] prev = prevV.get(DoubleWritable.class);
			double [] curr = currV.get(DoubleWritable.class);

			if(prev.length != curr.length){
				throw new IOException("size doesn't match! " + prev.length+ ":" + curr.length);
			}
			
			double change = 0;
			for (int i = 0; i < curr.length; i++) {
				double score1 = prev[i];
				double score2 = curr[i];
				change += (score1 - score2) * (score1 - score2);
			}
			 
			distance += Math.sqrt(change);
			
			return distance;
		}

		@Override
		public Text removeLable() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void iteration_complete(int iteration) {
			long curr_time = System.currentTimeMillis();
			System.out.println("iteration " + iteration + " takes " + 
					(curr_time-last_iter_end) + " total " + (curr_time-iter_start));
			last_iter_end = curr_time;
		}
	
	}
	
	public static class KmeansProjector implements Projector<IntWritable, GlobalUniqKeyWritable, GlobalUniqValueWritable> {

		private GlobalUniqKeyWritable key = new GlobalUniqKeyWritable();
		private GlobalUniqValueWritable value = null;
		private RecordReader<GlobalUniqKeyWritable, GlobalUniqValueWritable> reader;
		
		@Override
		public void configure(JobConf job) {
			//the inputformat can only be retrieved in the iterative job, and the initial value
			if(job.isIterative()){
				try{
					Path initcenterpath = new Path(job.getInitStatePath());
					FileSystem hdfs = FileSystem.get(job);
					long filelen = hdfs.getFileStatus(initcenterpath).getLen();
					
					InputSplit inputSplit = new FileSplit(initcenterpath, 0, filelen, job);
					reader = job.getDynamicInputFormat().getRecordReader(inputSplit, job, null);
					
					key = reader.createKey();
					value = reader.createValue();
					
					reader.next(key, value);
				}catch(IOException e){
					e.printStackTrace();
				}
			}
		}

		@Override
		public GlobalUniqKeyWritable project(IntWritable statickey) {
			return key;
		}

		@Override
		public GlobalUniqValueWritable initDynamicV(GlobalUniqKeyWritable dynamickey) {
			return value;
		}

		@Override
		public Partitioner<GlobalUniqKeyWritable, GlobalUniqValueWritable> getDynamicKeyPartitioner() {
			return new HashPartitioner<GlobalUniqKeyWritable, GlobalUniqValueWritable>();
		}

		@Override
		public org.apache.hadoop.mapred.Projector.Type getProjectType() {
			return Projector.Type.ONE2ALL;
		}
	}

	private static void printUsage() {
		System.out.println("iterkmeans <inStaticDir> <outDir> <k>");
		System.out.println(	"\t-p # of parittions\n" +
							"\t-i snapshot interval\n" +
							"\t-I # of iterations\n" +
							"\t-D initial dynamic path\n" +
							"\t-f input format\n" + 
							"\t-s run preserve job");
	}

	public static int main(String[] args) throws Exception {
		if (args.length < 3) {
			return -1;
		}
		
		int partitions = 0;
		int interval = 2;
		int max_iterations = Integer.MAX_VALUE;
		String init_dynamic = "";		
		
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
	    
	    String inStatic = other_args.get(0);
	    String output = other_args.get(1);
	    int k = Integer.parseInt(other_args.get(2));
	    
	    String iteration_id = "kmeans" + new Date().getTime();
	    
		/**
		 * the initialization job, for partition the data and workload
		 */
	    long initstart = System.currentTimeMillis();
	    
	    JobConf job1 = new JobConf(IterKmeans_BigCross.class);
	    String jobname1 = "Kmeans Init";
	    job1.setJobName(jobname1);
	    
	    job1.setDataDistribution(true);
	    job1.setIterativeAlgorithmID(iteration_id);		//must be unique for an iterative algorithm
	    job1.setInputFormat(TextInputFormat.class);
	    job1.setOutputFormat(TextOutputFormat.class);
	    TextInputFormat.addInputPath(job1, new Path(inStatic));
	    FileOutputFormat.setOutputPath(job1, new Path(output + "/substatic"));
	    job1.setInt("kmeans.cluster.k", k);
	    
	    //prepare the initial state data, it might be used later
	    job1.setInitStatePath(output + "/centers/iteration-0");
	    //job1.set("kmeans.init.center.path", output + "/centers/iteration-0");

	    job1.setMapperClass(DistributeDataMap.class);
		job1.setReducerClass(DistributeDataReduce.class);
		job1.setProjectorClass(KmeansProjector.class);

	    job1.setMapOutputKeyClass(IntWritable.class);
	    job1.setMapOutputValueClass(Text.class);
	    job1.setOutputKeyClass(IntWritable.class);
	    job1.setOutputValueClass(Text.class);
	    
	    job1.setNumReduceTasks(partitions);
	    
	    JobClient.runJob(job1);
	    
	    long initend = System.currentTimeMillis();
		Util.writeLog("iter.kmeans.log", "init job use " + (initend - initstart)/1000 + " s");
	    
	    /**
	     * start iterative application jobs
	     */
	    long itertime = 0;
    	long iterstart = System.currentTimeMillis();
    	
	    JobConf job = new JobConf(IterKmeans_BigCross.class);
	    String jobname = "Iter Kmeans Main ";
	    job.setJobName(jobname);
    
	    if(partitions == 0) partitions = Util.getTTNum(job);
	    
	    //set for iterative process   
	    job.setIterative(true);
	    job.setIterativeAlgorithmID(iteration_id);		//must be unique for an iterative algorithm
	    job.setLong(Parameters.ITER_START, iterstart);
	    
	    if(max_iterations == Integer.MAX_VALUE){
	    	job.setDistanceThreshold(1);
	    }else{
	    	job.setMaxIterations(max_iterations);
	    }
	    job.setCheckPointInterval(interval);	
	    
	    //kmeans always init with file
	    if(init_dynamic == ""){
	    	job.setInitWithFileOrApp(false);
	    	job.setInitStatePath(output + "/centers/iteration-0");
	    }else{
	    	job.setInitWithFileOrApp(false);
	    	job.setInitStatePath(init_dynamic);
	    }
	    job.setStaticDataPath(output + "/substatic");
	    job.setGlobalUniqValuePath(output + "/centers");
	    
	    job.setStaticInputFormat(IntTextKVInputFormat.class);
	    job.setDynamicInputFormat(GlobalDataInputFormat.class);		//MUST have this for the following jobs, even though the first job not need it
	    //job.setResultInputFormat(IntCenterKVInputFormat.class);	//for kmeans, we don't want to check the threshold, we only set max iterations
	    job.setOutputFormat(GlobalDataOutputFormat.class);
	    
	    FileInputFormat.addInputPath(job, new Path(output + "/substatic"));
	    FileOutputFormat.setOutputPath(job, new Path(output + "/result"));
	    
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(CenterArrayWritable.class);
	    
	    job.setIterativeMapperClass(KmeansMap.class);	
	    job.setIterativeReducerClass(KmeansReduce.class);
	    job.setProjectorClass(KmeansProjector.class);
	    
	    job.setNumReduceTasks(partitions);		
	    JobClient.runIterativeJob(job);

    	long iterend = System.currentTimeMillis();
    	itertime += (iterend - iterstart) / 1000;
    	Util.writeLog("iter.kmeans.log", "iteration computation takes " + itertime + " s");
	    
		return 0;
	}
}
