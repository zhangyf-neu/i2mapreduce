package org.apache.hadoop.examples.utils;



public class Parameters {

	/**
	 * @param args
	 */
	public static final String LOCAL_FILE_SHORTEST_LEN = "/tmp/shortestlen";
	public static final String COMPLETE_CHECK = "dir.complete";
	public static final String START_NODE = "start.node";
	public static final String LEN_RANGE = "length.range"; //for estimate sort partition
	public static int MACHINE_NUM = -1;
	public static final String ITER_START = "iter.start.time";
	
	//for pagerank
	public static final String PG_LAST_RANK_DIR = "pagerank.last.rank.dir";
	public static final String PG_CURR_RANK_DIR = "pagerank.curr.rank.dir";
	public static final String PG_TOTALPAGES = "pagerank.totalpages";
	public static final String PG_PERSONAL_START = "pagerank.personal.start";
	public static final float PG_DAMPING = (float)0.8;
	public static final float PG_RETAIN = (float)0.2;
	
	//for heatspread
	public static final String HS_RANK_DIR = "pagerank.rank.dir";
	public static final String HS_INDEGREE_DIR = "pagerank.indegree.dir";
	public static final String HS_TOTALPAGES = "pagerank.totalpages";
	
	//for kmeans
	public static final String KMEANS_INITCENTERS_DIR = "kmeans.initcenters.dir";
	public static final String KMEANS_CLUSTER_PATH = "kmeans.cluster.path";
	public static final String KMEANS_CLUSTER_K = "kmeans.cluster.k";
	public static final String KMEANS_THRESHOLD = "kmeans.threshold";
	
	//for averaging
	public static final String AV_PROCESSED_GRAPH = "averaging.processed.graph";
	public static final String AV_TOTALNODES = "averaging.totalnodes";
	public static final String AV_STARTNODES = "averaging.startnodes";
	public static final String AV_INIT_VALUE = "averaging.init.value";
	
	//for nmf
	public static final String NMF_MATRIX_A_ROW = "nmf.matrix.a.row";
	public static final String NMF_MATRIX_A_COL = "nmf.matrix.a.col";
	public static final String NMF_MATRIX_B_COL = "nmf.matrix.b.col";
	public static final String NMF_MATRIX_A = "nmf.matrix.a";
	public static final String NMF_MATRIX_B = "nmf.matrix.b";
	public static final String NMF_MATRIX_C = "nmf.matrix.c";
	public static final String NMF_OPERATOR1 = "nmf.operator1";
	public static final String NMF_OPERATOR2 = "nmf.operator2";
	public static final String SNMF_SPARSE = "snmf.sparseness";
	public static final String NMF_OUTPUT1 = "nmf.output1";
	public static final String NMF_OUTPUT2 = "nmf.output2";
	
	//for gen graph
	public static final String GEN_CAPACITY = "gengraph.capacity";
	public static final String GEN_ARGUMENT = "gengraph.argument";
	public static final String GEN_TYPE = "gengraph.type";
	public static final String GEN_OUT = "gengraph.output";

}
