package org.apache.hadoop.examples.iterative;

import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.utils.Parameters;
import org.apache.hadoop.examples.utils.Util;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.GlobalRecordable;
import org.apache.hadoop.io.GlobalUniqKeyWritable;
import org.apache.hadoop.io.GlobalUniqValueWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.CenterWritable;
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
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
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
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IntFloatKVLineRecordReader;
import org.apache.hadoop.mapred.lib.IntTextKVInputFormat;
import org.mortbay.log.Log;

public class IterKmeans {

	public static final int DIM = 10;
	
	
	public static class IntCenterKVInputFormat extends FileInputFormat<IntWritable, CenterWritable> implements
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
		public RecordReader<IntWritable, CenterWritable> getRecordReader(InputSplit split, JobConf job,
				Reporter reporter) throws IOException {
		    reporter.setStatus(split.toString());
		    return new IntCenterKVLineRecordReader(job, (FileSplit) split);
		}
	
	}
	
	public static class IntCenterKVLineRecordReader implements RecordReader<IntWritable, CenterWritable> {
		  private final LineRecordReader lineRecordReader;

		  private byte separator = (byte) '\t';

		  private LongWritable dummyKey;

		  private Text innerValue;
		  
		  public Class getKeyClass() { return IntWritable.class; }
		  
		  public IntWritable createKey() {
		    return new IntWritable();
		  }
		  
		  public CenterWritable createValue() {
		    return new CenterWritable();
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
		  public synchronized boolean next(IntWritable key, CenterWritable value)
		    throws IOException {
			IntWritable tKey = key;
			CenterWritable tValue = value;
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
	 	implements Mapper<Text, Text, IntWritable, Text> {
	
		public void map(Text key, Text value,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
			//System.out.println(key + "\t" + value);
			output.collect(new IntWritable(Integer.parseInt(key.toString())), value);
		}
	}
	
	public static class DistributeDataReduce extends MapReduceBase
		implements Reducer<IntWritable, Text, IntWritable, Text>{
		private JobConf conf;
		private String initCenterDir;
		private GlobalUniqValueWritable initCenters = new GlobalUniqValueWritable();
		private int k;
		private int centerIndex = 0;
		
		@Override
		public void configure(JobConf job){
			conf = job;
			k = job.getInt("kmeans.cluster.k", 0);
			initCenterDir = job.getInitStatePath();
		}
		
		@Override
		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<IntWritable, Text> output, Reporter report) throws IOException {
			
			/*
			TreeMap<Integer, Integer> dims = new TreeMap<Integer, Integer>();
			Text value = new Text();
			while(values.hasNext()) {
				value = values.next();
				String[] items = (((Text)values.next()).toString()).split(",");
				int dim_id = Integer.parseInt(items[0]);
				int dim_value = Integer.parseInt(items[1]);
				
				if(dims.containsKey(dim_id)){
					dims.put(dim_id, dims.get(dim_id) + dim_value);
				}else{
					dims.put(dim_id, dim_value);
				}			
			}
			
			StringBuilder builder = new StringBuilder();
			for(Map.Entry<Integer, Integer> entry : dims.entrySet()){
				int dim_id = entry.getKey();
				int dim_value = entry.getValue();
				
				builder.append(dim_id + "," + dim_value + " ");
			}
			
			String toOutput = builder.toString();
			*/
			
			//System.out.println(key + "\t" + values.next());
			
			while(values.hasNext()) {
				Text value = values.next();
				output.collect(key, value);	
				
				//randomly collect initial centers, only one reducer collect
				if(Util.getTaskId(conf) == 0 && (initCenters.size() < k) && (key.get() % 2 == 0)){
					TreeMap<Integer, Double> center = new TreeMap<Integer, Double>();
					String[] items = value.toString().split(" ");
					for(String item : items){
		    			int index = item.indexOf(",");
		    			if(index == -1){
		    				throw new IOException("wrong user data " + key);
		    			}
		    			
		    			int dim_id = Integer.parseInt(item.substring(0, index));
		    			double dim_value = Double.parseDouble(item.substring(index+1));
		    			center.put(dim_id, dim_value);
					}
					initCenters.put(new IntWritable(centerIndex), new CenterWritable(center));
					centerIndex++;
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
	
		private Map<Integer, TreeMap<Integer, Double>> centers = null;
		private TreeMap<Integer, Double> centernorm = new TreeMap<Integer, Double>();
		private int count = 0;
		
		private double similarity(TreeMap<Integer, Double> first, TreeMap<Integer, Float> second, double norm1){
			double cosine_sim = 0;
			double norm2 = 0;
			
			for(Map.Entry<Integer, Float> entry : second.entrySet()){
				Double dim_value1 = first.get(entry.getKey());
				if(dim_value1 != null){
					cosine_sim += dim_value1 * entry.getValue();
				}
				
				norm2 += entry.getValue() * entry.getValue();
			}
			
			cosine_sim = cosine_sim / (Math.sqrt(norm1) * Math.sqrt(norm2));
			
			return cosine_sim;
		}
		
		@Override
		public void map(IntWritable statickey, Text staticval,
				GlobalUniqKeyWritable dynamickey,
				GlobalUniqValueWritable dynamicvalue,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
			
			if(centers == null){
				centers = new HashMap<Integer, TreeMap<Integer, Double>>();
				MapWritable rawcenters = dynamicvalue.get();
				
				System.out.println("read centers " + dynamicvalue.toString());
				
				for(Map.Entry<Writable, Writable> entry : rawcenters.entrySet()){
					int centerid = ((IntWritable)entry.getKey()).get();
					TreeMap<Integer, Double> centerdata = ((CenterWritable)entry.getValue()).get();
					centers.put(centerid, centerdata);
					
					//compute the norms
					double norm = 0;
					for(Map.Entry<Integer, Double> entry2 : centerdata.entrySet()){
						norm += entry2.getValue() * entry2.getValue();
					}
					
					centernorm.put(centerid, norm);
				}
			}
			
			count++;
			reporter.setStatus(String.valueOf(count));

			TreeMap<Integer, Float> item_dims = new TreeMap<Integer, Float>();
			String line = staticval.toString();
    		StringTokenizer st = new StringTokenizer(line);
    		while(st.hasMoreTokens()){
    			String element = st.nextToken();
    			int index = element.indexOf(",");
    			if(index == -1){
    				throw new IOException("wrong user data " + statickey);
    			}
    			
    			int dim_id = Integer.parseInt(element.substring(0, index));
    			float dim_value = Float.parseFloat(element.substring(index+1));
    			
    			item_dims.put(dim_id, dim_value);
    		}
			
			double maxSim = -1;
			int simcluster = -1;
			for(Map.Entry<Integer, TreeMap<Integer, Double>> mean : centers.entrySet()){
				int centerid = mean.getKey();
				TreeMap<Integer, Double> centerdata = mean.getValue();
				double similarity = similarity(centerdata, item_dims, centernorm.get(centerid));

				if(statickey.get() == 3592){
					System.out.println(statickey + "\tsim: " + similarity + "\t" +  centernorm.get(centerid));
				
					double cosine_sim = 0;
					double norm2_sum = 0;
					double norm2 = 0;
					
					for(Map.Entry<Integer, Float> entry : item_dims.entrySet()){
						Double dim_value1 = centerdata.get(entry.getKey());
						if(dim_value1 != null){
							cosine_sim += dim_value1 * entry.getValue();
						}
						
						norm2_sum += entry.getValue() * entry.getValue();
						
						System.out.println(dim_value1 + "*" + entry.getValue() + "=" + cosine_sim + "\tnorm2 is " + norm2_sum);
					}
					
					cosine_sim = cosine_sim / (Math.sqrt(centernorm.get(centerid)) * Math.sqrt(norm2_sum));
					
					System.out.println(cosine_sim + "\t" + Math.sqrt(centernorm.get(centerid)) + "\t" + Math.sqrt(norm2_sum));
				}
				
				if(similarity > maxSim) {
					maxSim = similarity;
					simcluster = centerid;
				}
			}

			if(simcluster == -1){
				System.out.println("simcluster is -1 " + statickey + "\t" + simcluster + "\tvalue " + staticval);
			}
			output.collect(new IntWritable(simcluster), staticval);
			//System.out.println("output " + statickey + "\t" + simcluster);
		}

		@Override
		public Text removeLable() {
			// TODO Auto-generated method stub
			return null;
		}
	}
	
	public static class KmeansReduce extends MapReduceBase implements
		IterativeReducer<IntWritable, Text, IntWritable, CenterWritable> {
	
		private long iter_start;
		private long last_iter_end;
		
		@Override
		public void configure(JobConf job){
			iter_start = job.getLong(Parameters.ITER_START, 0);
			last_iter_end = iter_start;
		}
		
		@Override
		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<IntWritable, CenterWritable> output, Reporter report) throws IOException {
			
			//input key: cluster's mean  (whose mean has the nearest measure distance)
			//input value: user-id data

			int num = 0;
			TreeMap<Integer, Float> all_items = new TreeMap<Integer, Float>();
			//System.out.println("input key is: " + key);
			while(values.hasNext()) {		
				String line = values.next().toString();
				//System.out.println("input value is: " + line);
	    		StringTokenizer st = new StringTokenizer(line);
	    		while(st.hasMoreTokens()){
	    			String element = st.nextToken();
	    			int index = element.indexOf(",");
	    			if(index == -1){
	    				throw new IOException("wrong user data " + key);
	    			}
	    			
	    			int dim_id = Integer.parseInt(element.substring(0, index));
	    			float dim_value = Float.parseFloat(element.substring(index+1));
	    			
	    			Float oldv = all_items.get(dim_id);
	    			if(oldv == null){
	    				all_items.put(dim_id, dim_value);
	    			}else{
	    				all_items.put(dim_id, all_items.get(dim_id) + dim_value);
	    			}
	    		}
				num++;
			}

			TreeMap<Integer, Double> avg = new TreeMap<Integer, Double>();
			for(Map.Entry<Integer, Float> entry : all_items.entrySet()){
				int dim_id = entry.getKey();
				float dim_value = entry.getValue();
				double avgscore = (double)dim_value/num;
				avg.put(dim_id, avgscore);
			}
			
			output.collect(new IntWritable(key.get()), new CenterWritable(avg));
			
			//System.out.println("output " + key + "\t" + avg.get(avg.firstKey()));
		}

		@Override
		public float distance(IntWritable key, CenterWritable prevV,
				CenterWritable currV) throws IOException {
			float distance = 0;
			
			TreeMap<Integer, Double> prev = prevV.get();
			TreeMap<Integer, Double> curr = currV.get();

			if(prev.size() != curr.size()){
				throw new IOException("size doesn't match! " + prev.size() + ":" + curr.size());
			}
			
			double change = 0;
			for(int art : prev.keySet()){
				double score1 = prev.get(art);
				double score2 = curr.get(art);
				
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
	    
	    JobConf job1 = new JobConf(IterKmeans.class);
	    String jobname1 = "Kmeans Init";
	    job1.setJobName(jobname1);
	    
	    job1.setDataDistribution(true);
	    job1.setIterativeAlgorithmID(iteration_id);		//must be unique for an iterative algorithm
	    job1.setInputFormat(KeyValueTextInputFormat.class);
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
    	
	    JobConf job = new JobConf(IterKmeans.class);
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
	    job.setOutputValueClass(CenterWritable.class);
	    
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
