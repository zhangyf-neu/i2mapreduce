package org.apache.hadoop.examples.naive;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.examples.utils.Util;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;

public class NaiveKmeans_Lastfm {
	public static final int DIM = 40;
	
	/**
	 * initialize input data maper and reducer
	 */
	public static class InitInputMapper_Lastfm extends MapReduceBase
		implements Mapper<LongWritable, Text, IntWritable, Text> {

		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String[] items = line.split("\t");
			if(items.length != 4) {
				reporter.setStatus("Doesn't work:"+line);
				return;
			}
			int keyToEmit = items[0].hashCode();
			int artistid = items[1].hashCode();
			int plays = Integer.parseInt(items[3].trim());
			Text valueToEmit = new Text(String.valueOf(artistid) + "," + plays);
			output.collect(new IntWritable(keyToEmit), valueToEmit);
		}
	}
	
	public static class InitInputReducer_Lastfm extends MapReduceBase 
		implements Reducer<IntWritable, Text, IntWritable, Text>{
		private JobConf conf;
		private String initCenterDir;
		private HashMap<Integer, String> initCenters = new HashMap<Integer, String>();
		private int k;
		private int userIndex = 0;
		
		@Override
		public void configure(JobConf job){
			conf = job;
			k = job.getInt("kmeans.cluster.k", 0);
			initCenterDir = job.get("kmeans.init.center.dir");
		}
		
		@Override
		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<IntWritable, Text> output, Reporter report) throws IOException {
			
			//input key: user-id
			//input value: artist-id,plays
			//output key: user-id
			//output value: all artist-id,plays tuples seperated by semicolon
			
			TreeMap<Integer, Integer> artists = new TreeMap<Integer, Integer>();
			while(values.hasNext()) {
				String[] items = (((Text)values.next()).toString()).split(",");
				int artistid = Integer.parseInt(items[0]);
				int dim = Math.abs(artistid) % DIM;
				int plays = Integer.parseInt(items[1]);
				
				if(artists.containsKey(dim)){
					artists.put(dim, artists.get(dim) + plays);
				}else{
					artists.put(dim, plays);
				}			
			}

			//ensure dimension is 10
			if(artists.size() < DIM){
				for(int i=0; i< DIM; i++){
					if(artists.get(i) == null){
						artists.put(i, 0);
					}
				}
			}
			
			StringBuilder builder = new StringBuilder();
			Iterator<Integer> it = artists.keySet().iterator();
			while(it.hasNext()) {
				Integer artistID = it.next();
				int plays = artists.get(artistID);
				builder.append(artistID);
				builder.append(",");
				builder.append(plays);
				if(it.hasNext())
					builder.append(" ");
			}
			
			String toOutput = builder.toString();
			output.collect(key, new Text(toOutput));
			
			//randomly collect initial centers, only one reducer collect
			if(Util.getTaskId(conf) == 0 && (initCenters.size() < k) && (key.get() % 137 == 0)){
				initCenters.put(userIndex, toOutput);
				userIndex++;
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
				
				for(int i : initCenters.keySet()){
					bw.write(i + "\t" + initCenters.get(i) + "\n");
				}
				
				bw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * kmeans mapper and reducer
	 */
	public static class KmeansMapper_Lastfm extends MapReduceBase
		implements Mapper<IntWritable, Text, IntWritable, Text> {
		
		private HashMap<Integer, TreeMap<Integer, Double>> centers = new HashMap<Integer, TreeMap<Integer, Double>>();
		private int count = 0;
		//private HashMap<Integer, Integer> tt = new HashMap<Integer, Integer>();
		
		private void loadInitCenters(JobConf job, String centersDir) throws IOException{
			FileSystem fs;
			fs = FileSystem.get(job);

		    Path initDirPath = new Path(centersDir);
		    FileStatus[] files = fs.listStatus(initDirPath);
		    
		    for(FileStatus file : files){
		    	if(!file.isDir()){
		    		Path filePath = file.getPath();
			    	BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(filePath)));
			    	while(br.ready()){
			    		String line = br.readLine();
			    		String[] item = line.split("\t");
			    		int userid = Integer.parseInt(item[0]);
			    		
			    		TreeMap<Integer, Double> artists = new TreeMap<Integer, Double>();
			    		
			    		StringTokenizer st = new StringTokenizer(item[1]);
			    		while(st.hasMoreTokens()){
			    			String element = st.nextToken();
			    			int index = element.indexOf(",");
			    			if(index == -1){
			    				throw new IOException("wrong init centers " + item[0] + "\t" + item[1]);
			    			}
			    			
			    			int artistid = Integer.parseInt(element.substring(0, index));
			    			double playtimes = Double.parseDouble(element.substring(index+1));
			    			
			    			artists.put(artistid, playtimes);
			    		}
			    		
			    		centers.put(userid, artists);
			    	}
			    	br.close();
		    	}
		    	
		    }
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
		
		private double distance(TreeMap<Integer, Double> first, TreeMap<Integer, Integer> second){
			double distance = 0;
			for(int key : first.keySet()){
				double score1 = first.get(key);
				int score2 = second.get(key);
				
				distance += (score1 - score2) * (score1 - score2);
			}
			
			return Math.sqrt(distance);
		}
		
		@Override
		public void map(IntWritable key, Text value,
				OutputCollector<IntWritable, Text> output, Reporter report)
				throws IOException {
			//input key: user-id
			//input value: all artist-id,plays tuples seperated by semicolon
			//output key: cluster id  (whose mean has the nearest measure distance)
			//output value: user-id data
			
			count++;
			report.setStatus(String.valueOf(count));

			TreeMap<Integer, Integer> artists = new TreeMap<Integer, Integer>();
			String line = value.toString();
    		StringTokenizer st = new StringTokenizer(line);
    		while(st.hasMoreTokens()){
    			String element = st.nextToken();
    			int index = element.indexOf(",");
    			if(index == -1){
    				throw new IOException("wrong user data " + key);
    			}
    			
    			int artistid = Integer.parseInt(element.substring(0, index));
    			int playtimes = Integer.parseInt(element.substring(index+1));
    			
    			artists.put(artistid, playtimes);
    		}
			
			double minDistance = Double.MAX_VALUE;
			int simcluster = -1;
			for(Map.Entry<Integer, TreeMap<Integer, Double>> mean : centers.entrySet()){

				int centerid = mean.getKey();
				TreeMap<Integer, Double> centerdata = mean.getValue();
				double distance = distance(centerdata, artists);

				if(distance < minDistance) {
					minDistance = distance;
					simcluster = centerid;
				}
			}

			output.collect(new IntWritable(simcluster), value);
			
		}
	}
	
	public static class KmeansReducer_Lastfm extends MapReduceBase
		implements Reducer<IntWritable, Text, IntWritable, Text> {

		@Override
		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<IntWritable, Text> output, Reporter report) throws IOException {
			
			//input key: cluster's mean  (whose mean has the nearest measure distance)
			//input value: user-id data

			int num = 0;
			TreeMap<Integer, Integer> totalartist = new TreeMap<Integer, Integer>();
			while(values.hasNext()) {		
				String line = values.next().toString();
	    		StringTokenizer st = new StringTokenizer(line);
	    		while(st.hasMoreTokens()){
	    			String element = st.nextToken();
	    			int index = element.indexOf(",");
	    			if(index == -1){
	    				throw new IOException("wrong user data " + key);
	    			}
	    			
	    			int artistid = Integer.parseInt(element.substring(0, index));
	    			int playtimes = Integer.parseInt(element.substring(index+1));
	    			
	    			Integer oldv = totalartist.get(artistid);
	    			if(oldv == null){
	    				totalartist.put(artistid, playtimes);
	    			}else{
	    				totalartist.put(artistid, totalartist.get(artistid) + playtimes);
	    			}
	    		}
				num++;
			}
			
			String outputstring = "";
			TreeMap<Integer, Double> avgartist = new TreeMap<Integer, Double>();
			for(Map.Entry<Integer, Integer> entry : totalartist.entrySet()){
				int artist = entry.getKey();
				int value = entry.getValue();
				double avgscore = (double)value/num;
				avgartist.put(artist, avgscore);
				outputstring += artist + "," + avgscore + " ";
			}
			
			output.collect(key, new Text(outputstring));
		}
	}
	
	/**
	 * termination check job
	 */

	public static class TermCheckReducer_Lastfm extends MapReduceBase
		implements Reducer<Text, Text, Text, FloatWritable> {
	
		private OutputCollector<Text, FloatWritable> collector;
		private float change = 0;

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, FloatWritable> output, Reporter reporter)
				throws IOException {
			if(collector == null) collector = output;
			int i = 0;
			double distance = 0;
			
			TreeMap<Integer, Double> artists1 = new TreeMap<Integer, Double>();
			TreeMap<Integer, Double> artists2 = new TreeMap<Integer, Double>();
			
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
		    			
		    			int artistid = Integer.parseInt(element.substring(0, index));
		    			double playtimes = Double.parseDouble(element.substring(index+1));
		    			
		    			artists1.put(artistid, playtimes);
		    		}
				}else if(i == 2){
		    		StringTokenizer st = new StringTokenizer(line);
		    		while(st.hasMoreTokens()){
		    			String element = st.nextToken();
		    			int index = element.indexOf(",");
		    			if(index == -1){
		    				throw new IOException("wrong user data " + key);
		    			}
		    			
		    			int artistid = Integer.parseInt(element.substring(0, index));
		    			double playtimes = Double.parseDouble(element.substring(index+1));
		    			
		    			artists2.put(artistid, playtimes);
		    		}
		    		
					
					for(int art : artists1.keySet()){
						double score1 = artists1.get(art);
						double score2 = artists2.get(art);
						
						distance += (score1 - score2) * (score1 - score2);
					}
				}		
			}
			change += Math.sqrt(distance);
		}
		
		@Override
		public void close() throws IOException{
			collector.collect(new Text("sub change"), new FloatWritable(change));
		}
	}
	
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		if (args.length != 5) {
		      System.err.println("Usage: kmeans_lastfm <kmdata> <out dir> <k> <partitions> <maxiteration>");
		      System.exit(2);
		}
		
		String input = args[0];
		String output = args[1];
		int k = Integer.parseInt(args[2]);
		int partitions = Integer.parseInt(args[3]);
		int maxiteration = Integer.parseInt(args[4]);
		
		long initstart = System.currentTimeMillis();

		/**
		 * job to init the input data
		 */
		JobConf conf = new JobConf(NaiveKmeans.class);
		conf.setJobName("Kmeans-Init");
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(InitInputMapper_Lastfm.class);
		conf.setReducerClass(InitInputReducer_Lastfm.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output + "/initdata"));
		conf.setInt("kmeans.cluster.k", k);
		conf.set("kmeans.init.center.dir", output + "/iteration-0");
		conf.setNumReduceTasks(partitions);
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
			conf = new JobConf(NaiveKmeans.class);
			conf.setJobName("Kmeans-Main");

			conf.setOutputKeyClass(IntWritable.class);
			conf.setOutputValueClass(Text.class);
			//conf.setMapOutputKeyClass(IntWritable.class);

			conf.setMapperClass(KmeansMapper_Lastfm.class);
			conf.setReducerClass(KmeansReducer_Lastfm.class);
			conf.setInputFormat(SequenceFileInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);
			conf.set("kmeans.init.center.dir", output + "/iteration-" + (iteration-1));

			FileInputFormat.setInputPaths(conf, new Path(output + "/initdata"));
			FileOutputFormat.setOutputPath(conf, new Path(output + "/iteration-" + (iteration)));
			conf.setNumReduceTasks(partitions);

			JobClient.runJob(conf);

			long iterend = System.currentTimeMillis();
			itertime += (iterend - iterstart) / 1000;
			
			/******************** Kmeans Terminate Check Job ***********************/

			conf = new JobConf(NaiveKmeans.class);
			conf.setJobName("Kmeans-TermCheck");

			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(FloatWritable.class);
			conf.setMapOutputValueClass(Text.class);
			conf.setMapOutputKeyClass(Text.class);

			conf.setMapperClass(IdentityMapper.class);
			conf.setReducerClass(TermCheckReducer_Lastfm.class);

			conf.setInputFormat(KeyValueTextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);

			FileInputFormat.setInputPaths(conf, new Path(output + "/iteration-" + (iteration-1)), new Path(output + "/iteration-" + (iteration)));
			FileOutputFormat.setOutputPath(conf, new Path(output + "/termcheck-" + iteration));
			conf.setNumReduceTasks(1);

			JobClient.runJob(conf);
			
			long termend = System.currentTimeMillis();
			totaltime += (termend - iterstart) / 1000;
			
			Util.writeLog("naive.kmeans.log", "iteration computation " + iteration + " takes " + itertime + " s, include termination check takes " + totaltime);
			
		} while (iteration < maxiteration);
    }


}
