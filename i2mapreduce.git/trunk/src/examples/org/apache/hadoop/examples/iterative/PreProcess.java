package org.apache.hadoop.examples.iterative;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class PreProcess extends Configured implements Tool {
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 6) {
		      System.err.println("Usage: preprocess <in_static> <in_dynamic> <out_static> <out_dynamic> <partitions> <is ifle>");
		      System.exit(2);
		}

		String inStatic = args[0];
		String inDynamic = args[1];
		String outStatic = args[2];
		String outDynamic = args[3];
		int partitions = Integer.parseInt(args[4]);
		boolean isifile = Boolean.parseBoolean(args[5]);
		
		//distribute static data job2
		
	    JobConf job1 = new JobConf(getConf());
	    String jobname1 = "distribute static data";
	    job1.setJobName(jobname1);
	    
	    //job1.set(Common.SUBSTATIC, Common.SUBSTATIC_DIR);
	    job1.setInputFormat(KeyValueTextInputFormat.class);
	    TextInputFormat.addInputPath(job1, new Path(inStatic));
	    //job2.setOutputFormat(NullOutputFormat.class);
	    //job2.setOutputFormat(TextOutputFormat.class);
	    job1.setOutputFormat(TextOutputFormat.class);
	    FileOutputFormat.setOutputPath(job1, new Path(outStatic));
	    
	    job1.setJarByClass(PreProcess.class);
	    job1.setMapperClass(StaticDistributeMap.class);
	    if(isifile){
		    job1.setReducerClass(IFileStaticDistributeReduce.class);
	    }else{
		    job1.setReducerClass(StaticDistributeReduce.class);
	    }

	    job1.setMapOutputKeyClass(IntWritable.class);
	    job1.setMapOutputValueClass(Text.class);
	    job1.setOutputKeyClass(IntWritable.class);
	    job1.setOutputValueClass(Text.class);
	    
	    //new added
	    //job1.setProjectorClass(PageRankProjector.class);
	    //job1.setDynamicKeyPartitionerClass(HashPartitioner.class);					//state data partitioner
	    //job1.setPartitionerClass(IterativeInitHashPartitioner.class);				//static data partitioner, iterativeinithash partitioner need projector and hashpartitioner
	    
	    /**
	     * if partitions to0 small, which limit the map performance (since map is usually more haveyly loaded),
	     * we should partition the static data into 2*partitions, and copy reduce results to the other mappers in the same scale,
	     * buf first, we just consider the simple case, static data partitions == dynamic data partitions
	     */
	    job1.setNumReduceTasks(partitions);
	    
	    JobClient.runJob(job1);
	    
	    //################################################################
	    
	  //distribute state job2
	    JobConf job2 = new JobConf(getConf());
	    String jobname2 = "distribute state data";
	    job2.setJobName(jobname2);
	    
	    //job2.set(Common.SUBSTATE, Common.SUBSTATE_DIR);
	    job2.setInputFormat(KeyValueTextInputFormat.class);
	    TextInputFormat.addInputPath(job2, new Path(inDynamic));
	    //job2.setOutputFormat(NullOutputFormat.class);
	    //job2.setOutputFormat(TextOutputFormat.class);
	    job2.setOutputFormat(TextOutputFormat.class);
	    FileOutputFormat.setOutputPath(job2, new Path(outDynamic));

	    job2.setJarByClass(PreProcess.class);
	    job2.setMapperClass(StateDistributeMap.class);
	    if(isifile){
		    job2.setReducerClass(IFileStateDistributeReduce.class);
	    }else{
		    job2.setReducerClass(StateDistributeReduce.class);
	    }

	    job2.setMapOutputKeyClass(IntWritable.class);
	    job2.setMapOutputValueClass(FloatWritable.class);
	    job2.setOutputKeyClass(IntWritable.class);
	    job2.setOutputValueClass(FloatWritable.class);
	    //job2.setOutputKeyClass(NullWritable.class);
	    job2.setPartitionerClass(HashPartitioner.class);
	    
	    //job2.set(Common.VALUE_CLASS, valClass);
	    job2.setNumReduceTasks(partitions);		//set the partition number, guarantee the task granularity is fine enough, ensure each task can process a whole block
	    
	    JobClient.runJob(job2);
	    
	    return 0;

	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int res = ToolRunner.run(new Configuration(), new PreProcess(), args);
	    System.exit(res);
	}

}
