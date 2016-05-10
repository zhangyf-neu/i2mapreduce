package org.apache.hadoop.mapred.lib;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class IntDoubleKVInputFormat extends FileInputFormat<IntWritable, DoubleWritable> implements
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
	public RecordReader<IntWritable, DoubleWritable> getRecordReader(InputSplit split, JobConf job,
			Reporter reporter) throws IOException {
	    reporter.setStatus(split.toString());
	    return new IntDoubleKVLineRecordReader(job, (FileSplit) split);
	}

}
