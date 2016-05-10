package org.apache.hadoop.examples.incremental;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;


public class CompSeqFile {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		if (args.length < 2) {
			System.out.println("wrong number of input");
			return;
		}
		
		JobConf conf = new JobConf(CompSeqFile.class);
		FileSystem fs = FileSystem.getLocal(conf);
		Path path1 = new Path(args[0]);
		Path path2 = new Path(args[1]);
		
		SequenceFile.Reader reader1 = new SequenceFile.Reader(fs, path1, conf);
		SequenceFile.Reader reader2 = new SequenceFile.Reader(fs, path2, conf);
		
		LongWritable key1 = new LongWritable();
		FloatWritable val1 = new FloatWritable();
		LongWritable key2 = new LongWritable();
		FloatWritable val2 = new FloatWritable();
		
		float totaldiff = 0;
		float maxdiff = 0;
		int totalnum = 0;
		long maxkey = 0;
		try{
			while(reader1.next(key1, val1)){
				reader2.next(key2, val2);
				System.out.print(key1+":"+val1 + "\n" + key2 + ":" + val2);
				
				if(!key1.equals(key2)){
					throw new RuntimeException("key doesn't match! " + key1 + "\t" + key2);
				}
				
				float diff = Math.abs(val1.get() - val2.get());
				if(diff > maxdiff){
					maxdiff = diff;
					maxkey = key2.get();
				}
				
				totalnum++;
				totaldiff += diff;
			}
		}catch(Exception e){
			e.printStackTrace();
			float avg = totaldiff / totalnum;
			System.out.println("diff " + totaldiff + " avg " + avg + " max " + maxdiff + " on " + maxkey + " total " + totalnum);
			
			reader1.close();
			reader2.close();
			return;
		}

		reader1.close();
		reader2.close();
		
		float avg = totaldiff / totalnum;
		System.out.println("diff " + totaldiff + " avg " + avg + " max " + maxdiff + " on " + maxkey);
	}
}
