package org.apache.hadoop.examples.iterative;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.examples.utils.Util;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.IFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class IFileStateDistributeReduce extends MapReduceBase implements
	Reducer<IntWritable, Text, NullWritable, NullWritable> {

	private FSDataOutputStream out;
	private IFile.Writer<IntWritable, IntWritable> intWriter;
	private IFile.Writer<IntWritable, DoubleWritable> doubleWriter;
	private IFile.Writer<IntWritable, FloatWritable> floatWriter;
	private IFile.Writer<IntWritable, Text> textWriter;
	private String valClass;

	@Override
	public void configure(JobConf job){
		String outDir = job.get(Common.SUBSTATE);
		valClass = job.get(Common.VALUE_CLASS);
		FileSystem fs;
		try {
			fs = FileSystem.get(job);
			int taskid = Util.getTaskId(job);
			Path outPath = new Path(outDir + "/substate" + taskid);
			out = fs.create(outPath);
			
			if(valClass.equals("IntWritable")){
				intWriter = new IFile.Writer<IntWritable, IntWritable>(job, out, 
						IntWritable.class, IntWritable.class, null, null);
			}else if(valClass.equals("DoubleWritable")){
				doubleWriter = new IFile.Writer<IntWritable, DoubleWritable>(job, out, 
						IntWritable.class, DoubleWritable.class, null, null);
			}else if(valClass.equals("FloatWritable")){
				floatWriter = new IFile.Writer<IntWritable, FloatWritable>(job, out, 
						IntWritable.class, FloatWritable.class, null, null);
			}else if(valClass.equals("Text")){
				textWriter = new IFile.Writer<IntWritable, Text>(job, out, 
						IntWritable.class, Text.class, null, null);
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}

	@Override
	public void reduce(IntWritable arg0, Iterator<Text> values,
		OutputCollector<NullWritable, NullWritable> arg2, Reporter arg3)
		throws IOException {
		while(values.hasNext()){
			Text value = values.next();
			
			if(valClass.equals("IntWritable")){
				int out = Integer.parseInt(value.toString());
				intWriter.append(arg0, new IntWritable(out));
			}else if(valClass.equals("DoubleWritable")){
				double out = Double.parseDouble(value.toString());
				doubleWriter.append(arg0, new DoubleWritable(out));
			}else if(valClass.equals("FloatWritable")){
				float out = Float.parseFloat(value.toString());
				floatWriter.append(arg0, new FloatWritable(out));
			}else if(valClass.equals("Text")){
				String out = value.toString();
				textWriter.append(arg0, new Text(out));
			}	
			
			//arg2.collect(arg0, value);
		}	
	}
	
	@Override
	public void close(){
		try {
			if(valClass.equals("IntWritable")){
				intWriter.close();
			}else if(valClass.equals("DoubleWritable")){
				doubleWriter.close();
			}else if(valClass.equals("FloatWritable")){
				floatWriter.close();
			}else if(valClass.equals("Text")){
				textWriter.close();
			}
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
