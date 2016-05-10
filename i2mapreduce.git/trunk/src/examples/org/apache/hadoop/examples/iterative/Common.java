package org.apache.hadoop.examples.iterative;



public class Common {

	//for preprocess
	public static final String SUBSTATIC = "dir.substatic";
	public static final String SUBSTATE = "dir.substate";
	public static final String SUBSTATIC_DIR = "_substatic";
	public static final String SUBSTATE_DIR = "_substate";
	public static final String LOCAL_STATE = "/tmp/imapreduce/statedata";
	public static final String LOCAL_STATIC = "/tmp/imapreduce/staticdata";
	
	public static final String BASETIME_DIR = "dir.basetime";
	public static final String TIME_DIR = "dir.time";
	public static final int INDEX_BLOCK_SIZE = 100;
	public static final String VALUE_CLASS = "data.val.class";
	
	public static final String TOTAL_ENTRIES = "total.entries";
	
	//for shortestpath
	public static final String SP_START_NODE = "shortestpath.start.node";
	
	//for pagerank
	public static final String PG_TERM_THRESHOLD = "pagerank.termination.threshold";
	
	
	//for gen graph
	public static final String GEN_CAPACITY = "gengraph.capacity";
	public static final String GEN_ARGUMENT = "gengraph.argument";
	public static final String GEN_TYPE = "gengraph.type";
	public static final String GEN_OUT = "gengraph.output";
	
	public static final String IN_MEM = "store.in.memory";
}
