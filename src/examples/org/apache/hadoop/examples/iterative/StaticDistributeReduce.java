package org.apache.hadoop.examples.iterative;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.IFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class StaticDistributeReduce extends MapReduceBase implements
		//Reducer<IntWritable, Text, NullWritable, NullWritable> {
		Reducer<IntWritable, Text, IntWritable, Text> {
	private FSDataOutputStream out;
	private IFile.Writer<IntWritable, Text> writer;
	/*
	@Override
	public void configure(JobConf job){
		String outDir = job.get(Common.SUBSTATIC);
		FileSystem fs;
		try {
			fs = FileSystem.get(job);
			int taskid = Util.getTaskId(job);
			Path outPath = new Path(outDir + "/substatic" + taskid);
			out = fs.create(outPath);
			writer = new IFile.Writer<IntWritable, Text>(job, out, 
					IntWritable.class, Text.class, null, null);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	*/
	@Override
	public void reduce(IntWritable arg0, Iterator<Text> values,
			OutputCollector<IntWritable, Text> arg2, Reporter arg3)
			throws IOException {
		while(values.hasNext()){
			Text value = values.next();
			//writer.append(arg0, value);
			arg2.collect(arg0, value);
		}
	}
	/*
	@Override
	public void close(){
		try {
			writer.close();
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	*/
}
