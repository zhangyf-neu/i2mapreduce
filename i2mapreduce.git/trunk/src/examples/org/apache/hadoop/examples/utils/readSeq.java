package org.apache.hadoop.examples.utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.examples.incremental.CompSeqFile;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

public class readSeq {
	public static void main(String[] args) throws IOException {
		if (args.length < 2) {
			System.out.println("wrong number of input");
			return;
		}
		
		JobConf conf = new JobConf(CompSeqFile.class);
		FileSystem fs = FileSystem.getLocal(conf);
		Path path1 = new Path(args[0]);
		String output = args[1];
		
		SequenceFile.Reader reader1 = new SequenceFile.Reader(fs, path1, conf);
		BufferedWriter bw = new BufferedWriter(new FileWriter(output));
		
		LongWritable key1 = new LongWritable();
		FloatWritable val1 = new FloatWritable();
		
		try{
			while(reader1.next(key1, val1)){
				bw.write(key1+"\t"+val1 + "\n");
				bw.flush();
			}
		}catch(Exception e){
			e.printStackTrace();
		}

		reader1.close();
		bw.close();
	}
}
