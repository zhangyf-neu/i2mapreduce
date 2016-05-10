package org.apache.hadoop.mapred.lib;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Projector;

public class IdentityProjector<SK extends WritableComparable, DK extends WritableComparable, DV> implements Projector<SK, DK, DV> {

	@Override
	public void configure(JobConf job) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public DK project(SK statickey) {
		return (DK)statickey;
	}

	@Override
	public DV initDynamicV(DK dynamickey) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Partitioner<DK, DV> getDynamicKeyPartitioner() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public org.apache.hadoop.mapred.Projector.Type getProjectType() {
		// TODO Auto-generated method stub
		return null;
	}

}
