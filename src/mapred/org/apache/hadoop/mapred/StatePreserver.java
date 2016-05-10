package org.apache.hadoop.mapred;

import java.io.IOException;

/**
 * Yanfeng, 
 * used for preserve the state between mappers and reducers
 */
public interface StatePreserver<INKEY, OUTKEY, VALUE> {

	void preserve(INKEY inkey, OUTKEY outkey, VALUE value) throws IOException;
}
