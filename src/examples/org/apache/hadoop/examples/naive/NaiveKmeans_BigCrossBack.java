package org.apache.hadoop.examples.naive;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;


import org.apache.hadoop.examples.utils.Util;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.mortbay.log.Log;


public class NaiveKmeans_BigCrossBack {

	/**
	 * pick centers from initialize input data mapper and reducer
	 */
	public static class CenterExtractMapper extends MapReduceBase
		implements Mapper<LongWritable, Text, LongWritable, Text> {

		private int interval;
		private int count = 0;
		private int count2 = 0;
		
		public void configure(JobConf job){
			interval = job.getInt("kmeans.pick.interval", 0);
		}
		
		public void map(LongWritable key, Text value,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {
			//output a line every interval
			if(count % interval == 0){
				output.collect(key, value);
				reporter.setStatus(String.valueOf(++count2));
			}
			
			count++;
			
		}
	}
	
	public static class CenterExtractReducer extends MapReduceBase 
		implements Reducer<LongWritable, Text, NullWritable, Text>{
		
		private int k;
		private int outputclusters = 0;
		private int count = 0;
		
		@Override
		public void configure(JobConf job){
			k = job.getInt("kmeans.cluster.k", 0);
		}
		
		@Override
		public void reduce(LongWritable key, Iterator<Text> values,
				OutputCollector<NullWritable, Text> output, Reporter report) throws IOException {

			report.setStatus(String.valueOf(++count));
			
			if(outputclusters < k){
				Text line = new Text();
				while(values.hasNext()) {
					line = values.next();
					output.collect(NullWritable.get(), line);
					outputclusters++;
				}
			}
		}
	}
	
	/**
	 * kmeans mapper and reducer
	 */
	public static class KmeansMapper extends MapReduceBase
		implements Mapper<Text, Text, IntWritable, Text> {
		
		private HashMap<Integer, TreeMap<Integer, Double>> centers = new HashMap<Integer, TreeMap<Integer, Double>>();
		private int threshold = 0;
		private int iteration;
		private BufferedWriter clusterWriter;
		private int count = 0;
		private OutputCollector<IntWritable, Text> outCollector;
		
		private void loadInitCenters(JobConf job, String centersDir) throws IOException{
			FileSystem fs;
			fs = FileSystem.get(job);

		    Path initDirPath = new Path(centersDir);
		    FileStatus[] files = fs.listStatus(initDirPath);
		    
		    int center_id = 0;
		    for(FileStatus file : files){
		    	if(!file.isDir()){
		    		Path filePath = file.getPath();
			    	BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(filePath)));
			    	while(br.ready()){
			    		String line = br.readLine();
			    		
			    		String[] item = line.split("\t");
			    		if(item.length < 2) continue;
			    		TreeMap<Integer, Double> dimensions = new TreeMap<Integer, Double>();
			    		
			    		StringTokenizer st = new StringTokenizer(item[1]);
			    		while(st.hasMoreTokens()){
			    			String dim = st.nextToken();
			    			int index = dim.indexOf(",");
			    			if(index == -1){
			    				throw new IOException("wrong init centers " + item[0] + "\t" + item[1]);
			    			}
			    			
			    			int dim_id = Integer.parseInt(dim.substring(0, index));
			    			double dim_value = Double.parseDouble(dim.substring(index+1));
			    			
			    			dimensions.put(dim_id, dim_value);
			    		}
			    		
			    		centers.put(center_id, dimensions);
			    		/*
			    		System.out.println("centerid is " + center_id + "\t");
			    		for(Map.Entry<Integer, Double> entry : centers.get(center_id).entrySet()){
			    			System.out.println(entry.getKey() + "\t" + entry.getValue());
			    		}
			    		*/
			    		
			    		center_id++;
			    	}
			    	br.close();
		    	}
		    }
		    
		    System.out.println("center size: " + centers.size());
		}
		
		@Override
		public void configure(JobConf job){
			String initCenterDir = job.get("kmeans.init.center.dir");
			
			try {
				loadInitCenters(job, initCenterDir);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		private double distance(TreeMap<Integer, Double> first, TreeMap<Integer, Float> second){
			double euclidean = 0;
			
			for(Map.Entry<Integer, Float> entry : second.entrySet()){
				Double dim_value1 = first.get(entry.getKey());
				if(dim_value1 != null){
					euclidean += (dim_value1 - entry.getValue()) * (dim_value1 - entry.getValue());
				}
			}
			
			return Math.sqrt(euclidean);
		}
		
		private TreeMap<Integer, Float> getDims(String line) throws IOException{
			TreeMap<Integer, Float> item_dims = new TreeMap<Integer, Float>();
    		StringTokenizer st = new StringTokenizer(line);
    		while(st.hasMoreTokens()){
    			String element = st.nextToken();
    			int index = element.indexOf(",");
    			if(index == -1){
    				Log.info("no , found in line " + line);
    				continue;
    			}
    			
    			try{
    				int dim_id = Integer.parseInt(element.substring(0, index));
        			float dim_value = Float.parseFloat(element.substring(index+1));
        			item_dims.put(dim_id, dim_value);
    			}catch (NumberFormatException e){}
    		}
    		
    		return item_dims;
		}
		
		@Override
		public void map(Text key, Text value,
				OutputCollector<IntWritable, Text> output, Reporter report)
				throws IOException {
			//input key: item-id
			//input value: all dim_id,dim_value tuples seperated by semicolon
			//output key: cluster id  (whose mean has the nearest measure distance)
			//output value: item_id data
			
			count++;
			report.setStatus(String.valueOf(count));

			//System.out.println(key + "\t" + value);
			TreeMap<Integer, Float> item_dims = getDims(value.toString());
			//System.out.println(item_dims.size());
			
			double min_distance = Double.MAX_VALUE;
			int simcluster = -1;
			for(Map.Entry<Integer, TreeMap<Integer, Double>> mean : centers.entrySet()){
				int centerid = mean.getKey();
				TreeMap<Integer, Double> centerdata = mean.getValue();
				double distance = distance(centerdata, item_dims);
				
				//System.out.println("distance between " + centerid + " and " + key + " is " + distance);
				//System.out.println("The first entry is " + item_dims.firstEntry().getKey() + "\t" + item_dims.firstEntry().getValue());
				//System.out.println("and " + centerid + " is " + distance);

				if(distance < min_distance) {
					min_distance = distance;
					simcluster = centerid;
				}
			}

			if(simcluster == -1){
				System.out.println("similarity is smaller than -1\t" + key + "\t" + value);
			}
			
			//System.out.println("sim is " + simcluster);
			output.collect(new IntWritable(simcluster), value);
		}
	}
	
	public static class KmeansReducer extends MapReduceBase
		implements Reducer<IntWritable, Text, IntWritable, Text> {
		
		@Override
		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<IntWritable, Text> output, Reporter report) throws IOException {
			
			//input key: cluster's mean  (whose mean has the nearest measure distance)
			//input value: item-id data

			int num = 0;
			TreeMap<Integer, Float> center_dims = new TreeMap<Integer, Float>();
			while(values.hasNext()) {		
				String line = values.next().toString();
				
				//System.out.println(key + "\t" + line);
				
	    		StringTokenizer st = new StringTokenizer(line);
	    		while(st.hasMoreTokens()){
	    			String element = st.nextToken();
	    			int index = element.indexOf(",");
	    			if(index == -1){
	    				Log.info("no , found in line " + line);
	    				continue;
	    			}
	    			
	    			try{
		    			int dim_id = Integer.parseInt(element.substring(0, index));
		    			float dim_value = Float.parseFloat(element.substring(index+1));
		    			
		    			//System.out.println(dim_id + "\t" + dim_value);
		    			
		    			Float oldv = center_dims.get(dim_id);
		    			if(oldv == null){
		    				center_dims.put(dim_id, dim_value);
		    			}else{
		    				center_dims.put(dim_id, oldv + dim_value);
		    			}
	    			}catch (NumberFormatException e){}
	    		}
				num++;
			}
			
			String outputstring = "";
			for(Map.Entry<Integer, Float> entry : center_dims.entrySet()){
				int dim_id = entry.getKey();
				float dim_value = entry.getValue();
				double avg_value = (double)dim_value/num;
				outputstring += dim_id + "," + avg_value + " ";
			}
			
			output.collect(key, new Text(outputstring));
		}
	}
	
	/**
	 * termination check job
	 */

	public static class TermCheckReducer extends MapReduceBase
		implements Reducer<Text, Text, Text, DoubleWritable> {
	
		private OutputCollector<Text, DoubleWritable> collector;
		private double change = 0;

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, DoubleWritable> output, Reporter reporter)
				throws IOException {
			if(collector == null) collector = output;
			int i = 0;
			double distance = 0;
			
			TreeMap<Integer, Double> center_dims1 = new TreeMap<Integer, Double>();
			TreeMap<Integer, Double> center_dims2 = new TreeMap<Integer, Double>();
			
			while(values.hasNext()){
				i++;
				if(i > 2) System.out.println("something wrong");
				String line = values.next().toString();
	
				if(i == 1){
		    		StringTokenizer st = new StringTokenizer(line);
		    		while(st.hasMoreTokens()){
		    			String element = st.nextToken();
		    			int index = element.indexOf(",");
		    			if(index == -1){
		    				throw new IOException("wrong user data " + key);
		    			}
		    			
		    			int dim_id = Integer.parseInt(element.substring(0, index));
		    			double dim_value = Double.parseDouble(element.substring(index+1));
		    			
		    			center_dims1.put(dim_id, dim_value);
		    		}
				}else if(i == 2){
		    		StringTokenizer st = new StringTokenizer(line);
		    		while(st.hasMoreTokens()){
		    			String element = st.nextToken();
		    			int index = element.indexOf(",");
		    			if(index == -1){
		    				throw new IOException("wrong user data " + key);
		    			}
		    			
		    			int dim_id = Integer.parseInt(element.substring(0, index));
		    			double dim_value = Double.parseDouble(element.substring(index+1));
		    			
		    			center_dims2.put(dim_id, dim_value);
		    		}
		    		
					HashSet<Integer> keys = new HashSet<Integer>();
					keys.addAll(center_dims1.keySet());
					keys.addAll(center_dims2.keySet());
					
					for(Map.Entry<Integer, Double> entry : center_dims2.entrySet()){
						Double dim_value1 = center_dims1.get(entry.getKey());
						Double dim_value2 = entry.getValue();
						if(dim_value1 != null){
							distance += (dim_value1 - dim_value2) * (dim_value1 - dim_value2);
						}
					}
					System.out.println("total : " + distance);
				}		
			}
			change += distance;
		}
		
		@Override
		public void close() throws IOException{
			collector.collect(new Text("sub change"), new DoubleWritable(Math.sqrt(change)));
		}
	}
	
	private static void printUsage() {
		System.out.println("naivekmeans <inStaticDir> <outDir> <k>");
		System.out.println(	"\t-p # of parittions\n" +
							"\t-n # of items\n" +
							"\t-I max # of iterations");
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static int main(String[] args) throws IOException {
		if (args.length < 3) {
			System.out.println("ERROR: Wrong Input Parameters!");
	        printUsage();
	        return -1;
		}
		
		int partitions = 0;
		int items = 0;
		int max_iterations = Integer.MAX_VALUE;
		
		List<String> other_args = new ArrayList<String>();
		for(int i=0; i < args.length; ++i) {
		      try {
		          if ("-p".equals(args[i])) {
		        	partitions = Integer.parseInt(args[++i]);
		          } else if ("-n".equals(args[i])) {
		        	  items = Integer.parseInt(args[++i]);
		          } else if ("-I".equals(args[i])) {
		        	  max_iterations = Integer.parseInt(args[++i]);
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
	    String output = other_args.get(1);
	    int k = Integer.parseInt(other_args.get(2));
	    
	    if(k > items){
		      System.out.println("ERROR: number of clusters should be smaller than the number of items!");
		      return -1;
	    }
		
		long initstart = System.currentTimeMillis();

		/**
		 * job to extract the centers from the input data
		 */
		JobConf conf = new JobConf(NaiveKmeans.class);
		conf.setJobName("Kmeans-ExtractCenters");
		conf.setMapperClass(CenterExtractMapper.class);
		conf.setReducerClass(CenterExtractReducer.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setMapOutputKeyClass(LongWritable.class);
		conf.setOutputKeyClass(NullWritable.class);
		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output + "/iteration-0"));
		
		if(partitions == 0){
			partitions = Util.getTTNum(conf);
		}
		conf.setNumReduceTasks(1);
		if(items == 0){
			items = k;
		}
		
		conf.setInt("kmeans.cluster.k", k);
		conf.setInt("kmeans.pick.interval", (int)Math.floor(items/k));
		System.out.println(Math.floor(items/k));
		
		JobClient.runJob(conf);
		
		long initend = System.currentTimeMillis();
		Util.writeLog("naive.kmeans.log", "init job use " + (initend - initstart)/1000 + " s");
		
		long itertime = 0;
		long totaltime = 0;
		int iteration = 0;
		
		do {
			iteration++;
			/****************** Main Job ********************************/
			long iterstart = System.currentTimeMillis();;
			conf = new JobConf(NaiveKmeans_BigCrossBack.class);
			conf.setJobName("Kmeans-Main iteation " + iteration);

			conf.setOutputKeyClass(IntWritable.class);
			conf.setOutputValueClass(Text.class);
			//conf.setMapOutputKeyClass(IntWritable.class);

			conf.setMapperClass(KmeansMapper.class);
			conf.setReducerClass(KmeansReducer.class);
			conf.setInputFormat(KeyValueTextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);
			conf.set("kmeans.init.center.dir", output + "/iteration-" + (iteration-1));

			FileInputFormat.setInputPaths(conf, new Path(input));
			FileOutputFormat.setOutputPath(conf, new Path(output + "/iteration-" + (iteration)));
			conf.setNumReduceTasks(partitions);

			JobClient.runJob(conf);

			long iterend = System.currentTimeMillis();
			itertime += (iterend - iterstart) / 1000;
			
			/******************** Kmeans Terminate Check Job ***********************/

			conf = new JobConf(NaiveKmeans.class);
			conf.setJobName("Kmeans-TermCheck " + iteration);

			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(FloatWritable.class);
			conf.setMapOutputValueClass(Text.class);
			conf.setMapOutputKeyClass(Text.class);

			conf.setMapperClass(IdentityMapper.class);
			conf.setReducerClass(TermCheckReducer.class);

			conf.setInputFormat(KeyValueTextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);

			FileInputFormat.setInputPaths(conf, new Path(output + "/iteration-" + (iteration-1)), new Path(output + "/iteration-" + (iteration)));
			FileOutputFormat.setOutputPath(conf, new Path(output + "/termcheck-" + iteration));
			conf.setNumReduceTasks(1);

			JobClient.runJob(conf);
			
			long termend = System.currentTimeMillis();
			totaltime += (termend - iterstart) / 1000;
			
			Util.writeLog("naive.kmeans.log", "iteration computation " + iteration + " takes " + itertime + " s, include termination check takes " + totaltime);
			
		} while (iteration < max_iterations);
		
		return 1;
    }
}
