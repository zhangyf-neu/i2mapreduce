package org.apache.hadoop.mapred.lib;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.GlobalUniqKeyWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Projector;
import org.apache.hadoop.util.ReflectionUtils;

public class StaticDataPartitioner<SK extends WritableComparable, SV, DK extends WritableComparable, DV, K2, V2> implements Partitioner<SK, SV> {

	private static final Log LOG = LogFactory.getLog(StaticDataPartitioner.class);
	
	private Partitioner<DK, DV> dynamicKeyPartitioner;
	private Projector<SK, DK, DV> projector;
	private Projector.Type projectType;
	private int scala;
	private int dynamicPartitions;
	
	@Override
	public void configure(JobConf job) {
		projector = ReflectionUtils.newInstance(job.getProjectorClass(), job); 
		dynamicKeyPartitioner = projector.getDynamicKeyPartitioner();
		scala = job.getInt("mapred.iterative.data.scala", 1);
		projectType = projector.getProjectType();
		
		LOG.info("scala is " + scala + " dynamicPartitions " + dynamicPartitions);
	}

	@Override
	public int getPartition(SK key, SV value, int numPartitions) {
		//project to a dynamic key
		DK dynamicKey = projector.project(key);
		
		/**
		 * if it is only one globaluniqkey, there is no need to partition the static data for locality, so just partition
		 * the static data based on the static key. Otherwise, for locality, we partition the static data based on the dynamic 
		 * data result to see where it will be generated, and we assign the static data to that task id
		 */
		if(projectType == Projector.Type.ONE2ONE){
			return dynamicKeyPartitioner.getPartition(dynamicKey, null, numPartitions);
		}else if(projectType == Projector.Type.ONE2ALL){
			//we don't need to consider the partition, just use default partitioner
			return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
		}else if(projectType == Projector.Type.ONE2MUL){
			//LOG.info(key + " partitions is " + dynamicKeyPartitioner.getPartition(dynamicKey, null, numPartitions));
			
			return dynamicKeyPartitioner.getPartition(dynamicKey, null, numPartitions);
		}
		
		return -1;
	}
}
