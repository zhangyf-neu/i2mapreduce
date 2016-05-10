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
import java.util.Random;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.examples.utils.Util;
import org.apache.hadoop.fs.FSDataInputStream;
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


public class NaiveKmeans_BigCross {

	/**
	 * pick centers from initialize input data mapper and reducer
	 */
	
	public static class CenterExtractReducer extends MapReduceBase 
		implements Reducer<LongWritable, Text, Text, Text>{
		
		private int k;
		private int count = 0;
		private Random rand = new Random();
		private HashMap<Integer, String> out = new HashMap<Integer, String>();
		private OutputCollector<Text, Text> collector;
		
		@Override
		public void configure(JobConf job){
			k = job.getInt("kmeans.cluster.k", 0);
		}
		
		@Override
		public void reduce(LongWritable key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter report) throws IOException {
			if(collector == null) collector = output;
			
			report.setStatus(String.valueOf(count));
			Text line = new Text();
			while(values.hasNext()){
				line = values.next();
			}
			
			if(count < k){
				String linestr = line.toString();
				int startpos = linestr.indexOf(",") + 1;
				out.put(count, linestr.substring(startpos));
				System.out.println("put " + count + "\t" + line.toString());
				count++;
			}else{
				int i = rand.nextInt(count);
				if(i < k){
					String linestr = line.toString();
					int startpos = linestr.indexOf(",") + 1;
					out.put(i, linestr.substring(startpos));
					System.out.println("put " + i + "\t" + line.toString());
				}
				count++;
			}
		}
		
		@Override
		public void close() throws IOException{
			System.out.println("size " + out.size());
			for(Map.Entry<Integer, String> entry : out.entrySet()){
				collector.collect(new Text(String.valueOf(entry.getKey())), new Text(entry.getValue()));
			}
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

			HashMap<String, double[]> center_dims1 = new HashMap<String, double[]>();
			HashMap<String, double[]> center_dims2 = new HashMap<String, double[]>();
//			TreeMap<Integer, Double> center_dims1 = new TreeMap<Integer, Double>();
//			TreeMap<Integer, Double> center_dims2 = new TreeMap<Integer, Double>();
			while(values.hasNext()){
				i++;
				if(i > 2) System.out.println("something wrong");
				String line = values.next().toString();
				System.out.println(i + "\t" + line);
				if(i == 1){
					double []row = null;
					row=parseStringToVector(line, row);
					center_dims1.put(key.toString(), row);
				}else if(i == 2){ 
					double [] row1= center_dims1.get(key.toString());
					double [] row2=parseStringToVector(line);
					for (int j = 0; j < row2.length && j<row1.length; j++) {
						distance+=((row1[j]-row2[j])*(row1[j]-row2[j]));
						System.out.println("row1 " + row1[j] + " row2 " + row2[j] + " diff=" + distance);
					}
				}		
			}
			change += distance;
			System.out.println("change is " + change);
		}
		
		@Override
		public void close() throws IOException{
			collector.collect(new Text("sub change"), new DoubleWritable(Math.sqrt(change)));
		}
	}
	
	/**
		 * the Mapper class for K-means clustering
		 * 
		 * @author ybu
		 */
	public static class KMeansMapper extends MapReduceBase implements
				Mapper<LongWritable, Text, Text, Text> {
	
			private HashMap<String, double[]> centers = new HashMap<String, double[]>();
	
			private double rowBuffer[] = null;
	
			private Text idBuffer = new Text();
	
			private Text outputBuffer = new Text();
			
			private OutputCollector<Text, Text> outcollector;
//			private HashMap<Integer, TreeMap<Integer, Double>> centers = new HashMap<Integer, TreeMap<Integer, Double>>();
			//private TreeMap<Integer, Double> centernorm = new TreeMap<Integer, Double>();
			private void loadInitCenters(JobConf job, String centersDir) throws IOException{
				FileSystem fs;
				fs = FileSystem.get(job);
	
			    Path initDirPath = new Path(centersDir);
			    FileStatus[] files = fs.listStatus(initDirPath);
			     
			    for(FileStatus file : files){
			    	//double norm1 = 0;
			    	if(!file.isDir()){
			    		Path filePath = file.getPath();
				    	BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(filePath)));
			 
				    	while(br.ready()){
				    		String line[] = br.readLine().split("\t"); 
				    		centers.put(line[0],parseStringToVector2(line[1]));  
				    	}
				    	br.close();
			    	}
			    }
			    
//			    System.out.println("center size: " + centers.size());
			}
			
			public void configure(JobConf conf) {    
				String initCenterDir = conf.get("kmeans.init.center.dir");
				try {
					loadInitCenters(conf, initCenterDir);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}		
	 
			}
	
			private String printArray(double[] array){
				String out = new String();
				for (double i : array){
					out += i  + ",";
				}
				return out;
			}
			
			@Override
			public void map(LongWritable key, Text value,
					OutputCollector<Text, Text> output, Reporter reporter)
					throws IOException {
				if(outcollector == null) outcollector = output;
				
				if (centers == null || centers.isEmpty())
					throw new IOException("reducer output is null");
	
				String line = value.toString();
	
				// skip the first line
				if (line.indexOf("::") >= 0)
					return;
	
				// String dataString = line.substring(pos + 1);
				String dataString = line;
				// System.out.println("mapp string: " + dataString);
				double[] row = parseStringToVector(dataString, rowBuffer);
	
				Iterator<String> keys = centers.keySet().iterator();
				double minDistance = Double.MAX_VALUE;
				String minID = "";
	
				// find the cluster membership for the data vector
				while (keys.hasNext()) {
					String id = keys.next();
					double[] point = centers.get(id);
					
					//System.out.println(printArray(row));
					//System.out.println(printArray(point));
					
					double currentDistance = distance(row, point);
					
					//System.out.println("distance to " + id + " is " + currentDistance + " min distance is " + minDistance);
					
					if (currentDistance < minDistance) {
						minDistance = currentDistance;
						minID = id;
						// minValue = data;
					}
				}
	
				//System.out.println("minid " + minID);
				// output the vector (value) and cluster membership (key)
				idBuffer.clear();
				idBuffer.append(minID.getBytes(), 0, minID.getBytes().length);
	
				outputBuffer.clear();
				outputBuffer.append(dataString.getBytes(), 0, dataString.getBytes().length);
				output.collect(idBuffer, outputBuffer);
			}
			
			@Override
			public void close() throws IOException{
				Iterator<String> keys = centers.keySet().iterator();
				// find the cluster membership for the data vector
				while (keys.hasNext()) {
					String id = keys.next();
					String out = new String();
					double[] point = centers.get(id);
					for(int i=0; i<point.length; i++){
						if(i==0){
							out += point[i];
						}else{
							out += "," + point[i];
						}
					}
					
					outcollector.collect(new Text(id), new Text(out));
				}
			}
	
			/**
			 * get the Euclidean distance between two high-dimensional vectors
			 * 
			 * @param d1
			 * @param d2
			 * @return
			 */
			private double distance(double[] d1, double[] d2) {
				double distance = 0;
				int len = d1.length< d2.length ? d1.length: d2.length;
	
				for (int i = 0; i < len; i++) {
					distance += (d1[i] - d2[i]) * (d1[i] - d2[i]);
				}
	
				return Math.sqrt(distance);
			}
			
			
		}

	/**
			 * the reducer class for K-means clustering
			 * 
			 * @author ybu
			 * 
			 */
		public static class KMeansReducer extends MapReduceBase implements
					Reducer<Text, Text, Text, Text> {  
				private IntWritable result = new IntWritable();
		
				public void reduce(Text key, Iterator<Text> values,
						OutputCollector<Text, Text> output, Reporter reporter)
						throws IOException {
		
					double[] sum = null;
					long count = 0;
					if (values.hasNext()) {
						String data = values.next().toString();
						double[] row = parseStringToVector(data);
						sum = new double[row.length];
						accumulate(sum, row);
						count++;
					} else
						return;
		
					while (values.hasNext()) {
						String data = values.next().toString();
						double[] row = parseStringToVector(data);
						// Ssum = new double[row.length];
						if (row.length < sum.length) {
		//					System.out.println("less dim: " + data);
							continue;
						}
						accumulate(sum, row);
						count++;
					}
		
					// generate the new means
//					String result = sum[0] + ",";
					String result="";
					for (int i = 0; i <= sum.length - 1; i++) {
						sum[i] = sum[i] / count;
						if(i==sum.length-1)
							result += (sum[i] );
						else
							result += (sum[i] + ",");
					}
					result.trim();
		
					output.collect(key, new Text(result));
					System.out.println("cluster " + key + " has items " + count);
				}
			}

	private static void printUsage() {
		System.out.println("naivekmeans <inStaticDir> <outDir> <k>");
		System.out.println(	"\t-p # of parittions\n" +
							"\t-n # of items\n" +
							"\t-I max # of iterations");
	}
	
	/**
	 * parse a string to a high-dimensional double vector
	 * 
	 * @param line
	 * @return
	 */
	private static double[] parseStringToVector(String line, double[] row) {
		try {
			StringTokenizer tokenizer = new StringTokenizer(line, ",");
			int size = tokenizer.countTokens()-1;
			tokenizer.nextToken();
			
			if (row == null)
				row = new double[size];
			int i = 0;
			while (tokenizer.hasMoreTokens()) {
				String attribute = tokenizer.nextToken();
				row[i] = Double.parseDouble(attribute);
				i++;
			}
	
			return row;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * parse a string to a high-dimensional double vector
	 * 
	 * @param line
	 * @return
	 */
	private static double[] parseStringToVector(String line) {
		try {
			StringTokenizer tokenizer = new StringTokenizer(line, ",");
			int size = tokenizer.countTokens() - 1;
			tokenizer.nextToken();					//skip the first entry
			
			double[] row = new double[size];
			int i = 0;
			while (tokenizer.hasMoreTokens()) {
				String attribute = tokenizer.nextToken();
				row[i] = Double.parseDouble(attribute);
				i++;
			}
			
			return row;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	private static double[] parseStringToVector2(String line) {
		try {
			StringTokenizer tokenizer = new StringTokenizer(line, ",");
			int size = tokenizer.countTokens();
			
			double[] row = new double[size];
			int i = 0;
			while (tokenizer.hasMoreTokens()) {
				String attribute = tokenizer.nextToken();
				row[i] = Double.parseDouble(attribute);
				i++;
			}
			
			return row;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	private static void accumulate(double[] sum, double[] array) {
//		sum[0] = 0;
		for (int i = 0; i < sum.length; i++)
			sum[i] += array[i];
	}

	public static long getInputSize(JobConf conf, Path path) throws Exception {
		FileSystem dfs = FileSystem.get(conf);
		FileStatus[] files = dfs.listStatus(path);
	
		long size = 0;
	
		for (int i = 0; i < files.length; i++)
			size += files[i].getLen();
		return size;
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
		conf.setMapperClass(IdentityMapper.class);
		conf.setReducerClass(CenterExtractReducer.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		conf.setMapOutputKeyClass(LongWritable.class);
		conf.setOutputKeyClass(Text.class);
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
			conf = new JobConf(NaiveKmeans_BigCross.class);
			conf.setJobName("Kmeans-Main iteation " + iteration);
	
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			//conf.setMapOutputKeyClass(IntWritable.class);
	
			conf.setMapperClass(KMeansMapper.class);
			conf.setReducerClass(KMeansReducer.class);
			conf.setInputFormat(TextInputFormat.class);
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
