package org.apache.hadoop.mapred;

import org.apache.hadoop.io.WritableComparable;

public interface Projector<SK extends WritableComparable, DK extends WritableComparable, DV> extends JobConfigurable{
	public static enum Type { 
		ONE2ONE,
		ONE2ALL,
		ONE2MUL
	}

	DK project(SK statickey);
	DV initDynamicV(DK dynamickey);
	//DV removeLabel();
	Partitioner<DK, DV> getDynamicKeyPartitioner();
	Type getProjectType();
}
