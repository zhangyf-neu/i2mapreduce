package org.apache.hadoop.examples.incremental;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.IFile;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.examples.utils.Util;

public class UpdatePageRankGraph {
	
	public static class UpdateDataReduce extends MapReduceBase implements
		Reducer<LongWritable, Text, LongWritable, Text> {
		private Random rand = new Random();
		private OutputCollector<LongWritable, Text> collector = null;
		private long lastkey;
		private double changepercent;
		private int addpages = 0;
		private Set<Long> deletelist = new HashSet<Long>();
		private IFile.TrippleWriter<LongWritable, Text, Text> writer;
		private boolean delete;
		private int totalnum;
		private JobConf conf;
		
		@Override
		public void configure(JobConf job){
			conf = job;
			changepercent = job.getFloat("incr.pagerank.change.percent", -1);
			delete = job.getBoolean("pagerank.delta.contain.delete", false);
			if(delete){
				for(int i=0; i<10; i++){
					deletelist.add((long)100*i);
				}
			}
			totalnum = job.getInt("pagerank.delta.totalnum", -1);
	
			FileSystem fs;
			try {
				fs = FileSystem.get(job);
				Path deltapath = new Path(job.get("pagerank.delta.update.path") + "/part-" + Util.getTaskId(job));
				writer = new IFile.TrippleWriter<LongWritable, Text, Text>(job, fs, deltapath, 
						LongWritable.class, Text.class, Text.class, null, null);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	
		}
		
		@Override
		public void reduce(LongWritable key, Iterator<Text> values,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {
			if(collector == null){
				collector = output;
			}
			
			String outputv = "";
			while(values.hasNext()){
				outputv = values.next().toString();
			}
			
			//System.out.println("input: " + key + "\t" + outputv);
			
			if(deletelist.contains(key.get())){
				//System.out.println(key + "\t" + outputv + "\t-");
				writer.append(key, new Text(outputv), new Text("-"));
				return;
			}
			
			//randomlly change the linklist
			if(rand.nextDouble() < changepercent /*&& outputv.indexOf("0 ") == -1*/){
				//boolean change_type = rand.nextBoolean();		//update or add
				boolean change_type = true;
				
				if(change_type){
					//update
					writer.append(key, new Text(outputv), new Text("-"));
					//System.out.println(key + "\t" + outputv + "\t-");
					
					
					int randend = rand.nextInt(totalnum);
					while(randend == key.get()){
						randend = rand.nextInt(totalnum);
					}
					
					//write to the new structure file
					outputv += randend;
					//outputv += "27";
					
					output.collect(key, new Text(outputv));
					
					//write to the delta file
					writer.append(key, new Text(outputv), new Text("+"));
					//System.out.println(key + "\t" + outputv + "\t+");
				}else{
					//add
					addpages++;
				}
			}else{
				output.collect(key, new Text(outputv));
			}
		}
		
		@Override
		public void close() throws IOException{
			
			for(int i=1; i<=addpages; i++){
				long added = lastkey + conf.getNumMapTasks();
				//add ten random links for each node
				ArrayList<Integer> addlinks = new ArrayList<Integer>();
				String outputv = "";
				for(int j=0; j<10; j++){
					int randend = rand.nextInt(Integer.MAX_VALUE);
					while(randend == added){
						randend = rand.nextInt(Integer.MAX_VALUE);
					}
					addlinks.add(randend);
					outputv += randend;
				}
				collector.collect(new LongWritable(added), new Text(outputv));
				writer.append(new LongWritable(added), new Text(outputv), new Text("+"));
				
				System.out.println(added + "\t" + outputv + "\t+");
			}
			
			writer.close();
		}
	}
	
	private static void printUsage() {
		System.out.println("updatepr <OldStatic> <UpdateGraph> <DeltaGraph> " +
				"<partitions> <change percent> <contain delete> <totalnum>");
	}

	public static int main(String[] args) throws Exception {
		if (args.length < 7) {
			printUsage();
			return -1;
		}
	    
	    String oldStatic = args[0];
	    String updateoutput = args[1];
	    String deltaoutput = args[2];
	    int partitions = Integer.parseInt(args[3]);
	    float changepercent = Float.parseFloat(args[4]);
		boolean delete = Boolean.parseBoolean(args[5]);
		int totalnum = Integer.parseInt(args[6]);

		/**
		 * update the graph manually
		 */
	    long initstart = System.currentTimeMillis();
	    
	    JobConf job0 = new JobConf(UpdatePageRankGraph.class);
	    String jobname0 = "PageRank Update Generation";
	    job0.setJobName(jobname0);
	    
	    job0.setInputFormat(SequenceFileInputFormat.class);
	    job0.setOutputFormat(SequenceFileOutputFormat.class);
	    FileInputFormat.addInputPath(job0, new Path(oldStatic));
	    FileOutputFormat.setOutputPath(job0, new Path(updateoutput));

	    job0.setMapperClass(IdentityMapper.class);
	    job0.setReducerClass(UpdateDataReduce.class);

	    job0.setOutputKeyClass(LongWritable.class);
	    job0.setOutputValueClass(Text.class);
	    
	    job0.setFloat("incr.pagerank.change.percent", changepercent);		//the delta change percent of update/add
	    job0.setBoolean("pagerank.delta.contain.delete", delete);			//contain delete change or not
	    job0.set("pagerank.delta.update.path", deltaoutput);
	    job0.setInt("pagerank.delta.totalnum", totalnum);

	    job0.setNumReduceTasks(partitions);
	    
	    JobClient.runJob(job0);
	    
	    long initend = System.currentTimeMillis();
		//Util.writeLog("incr.pagerank.log", "update job use " + (initend - initstart)/1000 + " s");
		
		return 0;
	}
}
