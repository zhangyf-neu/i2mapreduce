package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.io.Closeable;

public interface IterativeMapper<SK, SV, DK, DV, K2, V2> extends JobConfigurable, Closeable{
	/**
	 * for loading the initial vector to priority-key-value buffer, user should
	 * use pkvBuffer.collect(priority, K, V) to initialize the priorityKVBuffer
	 * @param pkvBuffer
	 * @throws IOException
	 */
	void map(SK statickey, SV staticval, DK dynamickey, DV dynamicvalue, OutputCollector<K2, V2> output, Reporter reporter)
	  throws IOException;
	V2 removeLable();
}