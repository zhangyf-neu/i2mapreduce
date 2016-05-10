package org.apache.hadoop.examples.utils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import jsc.distributions.Lognormal;

//import nmf.IntIntPairWritable;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class genGraphReduce extends MapReduceBase implements
		Reducer<LongWritable, Text, IntWritable, Text> {

	private int argument;
	private int capacity;
	private int subcapacity;
	private String type;
	private BufferedWriter out;			//normal static
	private BufferedWriter out2;		//iterative static
	private BufferedWriter out3;		//iterative state
	private boolean done = false;
	private int taskid;
	private JobConf conf;
	//private IFile.Writer<IntIntPairWritable, FloatWritable> writer;
	
	public static final double SP_EDGE_LOGN_MU = 1.5;
	public static final double SP_EDGE_LOGN_SIGMA = 1.0;
	public static final double SP_WEIGHT_LOGN_MU = 0.4;
	public static final double SP_WEIGHT_LOGN_SIGMA = 1.2;
	public static final double PG_EDGE_LOGN_MU = -1;
	public static final double PG_EDGE_LOGN_SIGMA = 2.3;
	public static final int KM_FEATURES_SCALE = 10000;
	public static final int KM_WEIGHT_SCALE = 500;
	public static final int KM_NORMAL_M = 20;
	public static final int KM_NORMAL_D = 10;
	public static final int WEIGHT_SCALE = 100;
	public static final double NMF_LOGN_MU = 1;
	public static final double NMF_LOGN_SIGMA = 1;
	
	@Override
	public void configure(JobConf job){
		argument = job.getInt(Parameters.GEN_ARGUMENT, 0);
		capacity = job.getInt(Parameters.GEN_CAPACITY, 0);
		subcapacity = capacity / Util.getTTNum(job);
		type = job.get(Parameters.GEN_TYPE);
		String outdir = job.get(Parameters.GEN_OUT);
		try {
			FileSystem fs = FileSystem.get(job);
			
			FSDataOutputStream os;
			FSDataOutputStream os2;
			FSDataOutputStream os3;
			
			if(type.equals("nmf")){
				os = fs.create(new Path(outdir + "/part" + Util.getTaskId(job)));
				out = new BufferedWriter(new OutputStreamWriter(os));
			}else if(type.equals("power")){
				os = fs.create(new Path(outdir + "/M/part" + Util.getTaskId(job)));
				os2 = fs.create(new Path(outdir + "/N/part" + Util.getTaskId(job)));
				os3 = fs.create(new Path(outdir + "/Niter/subrank" + Util.getTaskId(job)));
				out = new BufferedWriter(new OutputStreamWriter(os));
				out2 = new BufferedWriter(new OutputStreamWriter(os2));
				//writer = new IFile.Writer<IntIntPairWritable, FloatWritable>(job, os3, 
						//IntIntPairWritable.class, FloatWritable.class, null, null);
			}else if(type.equals("gimv")){
				os = fs.create(new Path(outdir + "/matrix/part" + Util.getTaskId(job)));
				os2 = fs.create(new Path(outdir + "/vector/part" + Util.getTaskId(job)));
				out = new BufferedWriter(new OutputStreamWriter(os));
				out2 = new BufferedWriter(new OutputStreamWriter(os2));
				//writer = new IFile.Writer<IntIntPairWritable, FloatWritable>(job, os3, 
						//IntIntPairWritable.class, FloatWritable.class, null, null);
			}else{
				os = fs.create(new Path(outdir + "/normalstatic/part" + Util.getTaskId(job)));
				os2 = fs.create(new Path(outdir + "/iterativestatic/part" + Util.getTaskId(job)));
				os3 = fs.create(new Path(outdir + "/iterativestate/part" + Util.getTaskId(job)));
				out = new BufferedWriter(new OutputStreamWriter(os));
				out2 = new BufferedWriter(new OutputStreamWriter(os2));
				out3 = new BufferedWriter(new OutputStreamWriter(os3));
			}
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		taskid = Util.getTaskId(job);
		conf = job;
	}
	
	@Override
	public void reduce(LongWritable key, Iterator<Text> values,
			OutputCollector<IntWritable, Text> output, Reporter reporter)
			throws IOException {
		
		if(!done){
			if(type.equals("sp")){
				Lognormal logn = new Lognormal(SP_EDGE_LOGN_MU, SP_EDGE_LOGN_SIGMA);
				
				int base = subcapacity * taskid;
				for(int i=0; i<subcapacity; i++){
					int index = base + i;
					
					double rand = logn.random();			
					int num_link = (int) Math.ceil(rand);
					
					while(num_link > capacity / 2){
						rand = logn.random();
						num_link = (int)Math.ceil(rand);
					}

					if(index == argument){
						out.write(String.valueOf(index)+"\tf0:");
						out3.write(String.valueOf(index)+"\tf:0\n");
					}else{
						out.write(String.valueOf(index)+"\tp:");
						out3.write(String.valueOf(index)+"\tv:" + Integer.MAX_VALUE + "\n");
					}
					out2.write(String.valueOf(index)+"\t");
					
					Random r = new Random();
					Lognormal logn2 = new Lognormal(SP_WEIGHT_LOGN_MU, SP_WEIGHT_LOGN_SIGMA);
					
					ArrayList<Integer> links = new ArrayList<Integer>(num_link);
					for(int j=0; j< num_link; j++){
						int link = r.nextInt(capacity);
						int trys = 0;
						while(links.contains(link) && trys < 10){
							link = r.nextInt(capacity);
							trys++;
						}
						if(trys == 10) continue;
						
						links.add(link);
						double rand2 = logn2.random();
						
						int weight = 100 - (int)Math.ceil(rand2);
						if(weight <= 0) weight = 1;
						
						//System.out.println(weight);
						out.write(String.valueOf(link) + "," + String.valueOf(weight));
						out2.write(String.valueOf(link) + "," + String.valueOf(weight));
						if(j < num_link-1){
							out.write(" ");
							out2.write(" ");
						}
					}
					out.write("\n");
					out2.write("\n");
					out.flush();
					out2.flush();
					out3.flush();
				}
			}else if(type.equals("pg")){
				Lognormal logn = new Lognormal(PG_EDGE_LOGN_MU, PG_EDGE_LOGN_SIGMA);

				int base = subcapacity * taskid;
				
				//determine the most number of links one node could have
				int mostnum = subcapacity;
				
				/*
				if(tasknum > 8){
					mostnum = avgcapacity * 8;
				}else{
					mostnum = capacity;
				}
				*/
				
				for(int i=0; i<subcapacity; i++){
					reporter.setStatus(String.valueOf(i));
					int index = base + i;
					
					double rand = logn.random();

					int num_link = (int)Math.round(rand);
					
					while(num_link > mostnum){
						rand = logn.random();
						num_link = (int)Math.round(rand);
					}
					if(num_link <= 0) num_link = 1;
			
					/*
					if(index < argument){
						out.write(String.valueOf(index)+"\t" + initial + ":");
					}else{
						out.write(String.valueOf(index)+"\t0:");
					}
					
					out2.write(String.valueOf(index)+"\t");
					out3.write(String.valueOf(index)+"\t1\n");
					

					
					//System.out.println(prob);
					Random r = new Random();

					ArrayList<Integer> links = new ArrayList<Integer>(num_link);
					for(int j=0; j< num_link; j++){
						int link = r.nextInt(capacity);
						
						int trys = 0;
						while(links.contains(link)  && trys < 10){
							link = r.nextInt(capacity);
							trys++;
						}
						if(trys == 10) continue;
						links.add(link);

						//System.out.println(weight);
						out.write(String.valueOf(link));
						out2.write(String.valueOf(link));
						if(j < num_link-1){
							out.write(" ");
							out2.write(" ");
						}
			
					}
					out.write("\n");		
					out2.write("\n");	
					*/
					
					Random r = new Random();
					ArrayList<Integer> links = new ArrayList<Integer>(num_link);
					for(int j=0; j< num_link; j++){
						int link = r.nextInt(capacity);
						
						/*
						//this is for the case, no replicated link for a node, but this might result in 
						//long running time, since we should contain the whole linklist in memory
						int trys = 0;
						while(links.contains(link) && trys < 10){
							link = r.nextInt(capacity);
							trys++;
						}
						if(trys == 10) continue;
						*/
						
						links.add(link);

						//System.out.println(weight);
						out.write(index + "\t" + String.valueOf(link) + "\n");
						out2.write(index + "\t" + String.valueOf(link) + "\n");
					}
					
					out.flush();
					out2.flush();
					out3.flush();
				}
			}else if(type.equals("km")){
				//argument is the number of dims
				int posible_dims = argument;
				
				Random r = new Random();
				int base = subcapacity * taskid;
				
				for(int i=0; i<subcapacity; i++){
					int index = base + i;
					
					//choose the number of dims of a point
					int num_dim = r.nextInt(posible_dims) + 1;
					Set<Integer> choosed_dims = new HashSet<Integer>(num_dim);
					
					String point = new String(); 
					for(int j=0; j<num_dim; j++){
						int dim_id = r.nextInt(posible_dims) + 1;
						
						while(choosed_dims.contains(dim_id)){
							dim_id = r.nextInt(posible_dims) + 1;
						}
						choosed_dims.add(dim_id);
						
						int dim_value = r.nextInt(KM_WEIGHT_SCALE) + 1;
						point += dim_id + "," + dim_value + " ";
					}

					out.write(index + "\t" + point + "\n");
					//out2.write(index + "\t" + point + "\n");
					//out3.write(index + "\t" + point + "\n");	
					
					out.flush();
					//out2.flush();
					//out3.flush();
				}
			}else if(type.equals("nmf")){
				//int subsetnum = capacity / conf.getNumMapTasks();
				int ntasks = conf.getNumMapTasks();
				//int offset = subsetnum * taskid; 
				
				Lognormal logn = new Lognormal(NMF_LOGN_MU, NMF_LOGN_SIGMA);
				
				for(int i=taskid; i<capacity; i=i+ntasks){			//capacity is m
					float[] rand = new float[argument];	//argument is n

					float max = 0;
					for(int j=0; j<argument; j++){
						rand[j] = (float)logn.random();
						
						if(rand[j] > max){
							max = rand[j];
						}	
					}
					
					max = max+1;
					out.write(i + "\t");
					for(int j=0; j<argument; j++){
						out.write(j + "," + rand[j]/max + " ");
					}
					
					out.write("\n");
					out.flush();
					reporter.setStatus(String.valueOf(i));
				}
			}else if(type.equals("power")){

				int ntasks = conf.getNumMapTasks();
				int dim = argument;
				Random rand = new Random();
				
				for(int i=taskid; i<capacity; i=i+ntasks){			//capacity is m
					HashSet<Integer> dims = new HashSet<Integer>();
					
					for(int j=0; j<dim; j++){
						int index = rand.nextInt(capacity);
						dims.add(index);
					}
					
					for(int j : dims){
						float val = rand.nextFloat();
						out.write(i + "," + j + "\t" + val + "\n");
						out2.write(i + "," + j + "\t" + val + "\n");
						//writer.append(new IntIntPairWritable(i,j), new FloatWritable(val));
					}
					
					out.flush();
					out2.flush();
					reporter.setStatus(String.valueOf(i));
				}
			}
			//capacity: # of matrix rows and columns
			//argument: # of dims, # of meaningful columns
			else if(type.equals("gimv")){
				int ntasks = conf.getNumMapTasks();
				int dim = argument;
				Random rand = new Random();
				
				System.out.println("i am writing!");
				
				for(int i=taskid; i<capacity; i=i+ntasks){			//capacity is m
					HashSet<Integer> dims = new HashSet<Integer>();
					
					for(int j=0; j<dim; j++){
						int index = rand.nextInt(capacity);
						dims.add(index);
					}
					
					Arrays.sort(dims.toArray());
					for(int j : dims){
						float val = rand.nextFloat();
						out.write(i + " " + j + " " + val + "\n");
						//writer.append(new IntIntPairWritable(i,j), new FloatWritable(val));
						System.out.println(i + " " + j + " " + val + "\n");
					}
					
					out.flush();
					out2.write(i + " 0.001\n");
					reporter.setStatus(String.valueOf(i));
				}
			}
			
			out.close();
			if(out2 != null) out2.close();
			if(out3 != null) out3.close();
			//if(writer != null) writer.close();
			done = true;
		}
	}


}
