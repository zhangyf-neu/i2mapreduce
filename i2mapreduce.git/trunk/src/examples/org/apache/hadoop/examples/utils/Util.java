package org.apache.hadoop.examples.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;


public class Util {

	public static void writeInt(Configuration conf, String filename, int content) throws IOException{
		
		FileSystem hdfs = FileSystem.get(conf);	
		
		FSDataOutputStream fileOut = hdfs.create(new Path(filename));
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fileOut));
		writer.write(String.valueOf(content));
		writer.close();
		fileOut.close();
	}
		
	public static int readInt(Configuration conf, String filename) throws IOException{
		Path object = new Path(filename);
		FileSystem hdfs = object.getFileSystem(conf);
		int result = 0;
		
		if(hdfs.isFile(object)){
			FSDataInputStream fis = hdfs.open(object);
			BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
						
			while(reader.ready()){
				result += Double.parseDouble(reader.readLine());
			}

			reader.close();
			fis.close();
		}else if(hdfs.listStatus(object) != null){
			FileStatus[] status = hdfs.listStatus(object);
						
			for(int i=0; i<status.length; i++){
				FSDataInputStream fis = hdfs.open(status[i].getPath());
				BufferedReader reader = new BufferedReader(new InputStreamReader(fis));				
				
				while(reader.ready()){
					result += Integer.parseInt(reader.readLine());
				}

				reader.close();
				fis.close();				
			}
		}
		
		return result;
	}
	
	public static long readLong(Configuration conf, String filename) throws IOException{
		Path object = new Path(filename);
		FileSystem hdfs = object.getFileSystem(conf);
		long result = 0;
		
		if(hdfs.isFile(object)){
			FSDataInputStream fis = hdfs.open(object);
			BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
						
			while(reader.ready()){
				result += Long.parseLong(reader.readLine());
			}

			reader.close();
			fis.close();
		}else if(hdfs.listStatus(object) != null){
			FileStatus[] status = hdfs.listStatus(object);
						
			for(int i=0; i<status.length; i++){
				FSDataInputStream fis = hdfs.open(status[i].getPath());
				BufferedReader reader = new BufferedReader(new InputStreamReader(fis));				
				
				while(reader.ready()){
					result += Long.parseLong(reader.readLine());
				}

				reader.close();
				fis.close();				
			}
		}
		
		return result;
	}
	
	public static void writeDouble(Configuration conf, String filename, double content) throws IOException{
		FileSystem hdfs = FileSystem.get(conf);	
		FSDataOutputStream fileOut = hdfs.create(new Path(filename), true);
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fileOut));
		writer.write(String.valueOf(content));
		writer.close();
		fileOut.close();
	}
	
	public static void writeFloat(Configuration conf, String filename, float content) throws IOException{
		FileSystem hdfs = FileSystem.get(conf);	
		FSDataOutputStream fileOut = hdfs.create(new Path(filename), true);
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fileOut));
		writer.write(String.valueOf(content));
		writer.close();
		fileOut.close();
	}
	
	public static double readDouble(Configuration conf, String filename) throws IOException{
		Path object = new Path(filename);
		FileSystem hdfs = object.getFileSystem(conf);
		double result = 0;
		
		if(hdfs.isFile(object)){
			FSDataInputStream fis = hdfs.open(object);
			BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
						
			while(reader.ready()){
				result += Double.parseDouble(reader.readLine());
			}

			reader.close();
			fis.close();
		}else if(hdfs.listStatus(object) != null){
			FileStatus[] status = hdfs.listStatus(object);
						
			for(int i=0; i<status.length; i++){
				FSDataInputStream fis = hdfs.open(status[i].getPath());
				BufferedReader reader = new BufferedReader(new InputStreamReader(fis));				
				
				while(reader.ready()){
					result += Double.parseDouble(reader.readLine());
				}

				reader.close();
				fis.close();				
			}
		}
		
		return result;
	}
	
	public static float readFloat(Configuration conf, String filename) throws IOException{
		Path object = new Path(filename);
		FileSystem hdfs = object.getFileSystem(conf);
		FileStatus[] status = hdfs.listStatus(object);
		float result = 0;
		
		for(int i=0; i<status.length; i++){
			FSDataInputStream fis = hdfs.open(status[i].getPath());
			BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
			
			
			while(reader.ready()){
				result += Float.parseFloat(reader.readLine());
			}

			reader.close();
			fis.close();				
		}
		
		return result;
	}
	
	public static void writeLong(Configuration conf, String filename, long content) throws IOException{
		FileSystem hdfs = FileSystem.get(conf);	
		FSDataOutputStream fileOut = hdfs.create(new Path(filename), true);
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fileOut));
		writer.write(String.valueOf(content));
		writer.close();
		fileOut.close();
	}
	
	public static void writeLocalInt(String filename, int content) throws IOException{
		Date date = new Date();
        //Time formate 14.36.33
		SimpleDateFormat formatter = new SimpleDateFormat("HH.mm.ss");
        String s = formatter.format(date);

		FileWriter file = new FileWriter(filename, true);
		BufferedWriter out = new BufferedWriter(file);
		out.write(s + "\t" + String.valueOf(content)+"\n");
		out.close();
		file.close();
	}
	
	public static void writeLocalLong(String filename, long content) throws IOException{
		Date date = new Date();
        //Time formate 14.36.33
		SimpleDateFormat formatter = new SimpleDateFormat("HH.mm.ss");
        String s = formatter.format(date);

		FileWriter file = new FileWriter(filename, true);
		BufferedWriter out = new BufferedWriter(file);
		out.write(s + "\t" + String.valueOf(content)+"\n");
		out.close();
		file.close();
	}
	
	public static void writeLocalDouble(String filename, double content) throws IOException{
		Date date = new Date();
        //Time formate 14.36.33
		SimpleDateFormat formatter = new SimpleDateFormat("HH.mm.ss");
        String s = formatter.format(date);

		FileWriter file = new FileWriter(filename, true);
		BufferedWriter out = new BufferedWriter(file);
		out.write(s + "\t" + String.valueOf(content)+"\n");
		out.close();
		file.close();
	}
	
	public static void writeLog(String filename, String content) throws IOException{
		Date date = new Date();
        //Time formate 14.36.33
		SimpleDateFormat formatter = new SimpleDateFormat("HH.mm.ss");
        String s = formatter.format(date);

		FileWriter file = new FileWriter(filename, true);
		BufferedWriter out = new BufferedWriter(file);
		out.write(s + "\t" + content +"\n");
		out.close();
		file.close();
	}
	
	public static int getTaskId(JobConf conf) throws IllegalArgumentException{
	       if (conf == null) {
	           throw new NullPointerException("conf is null");
	       }

	       String taskId = conf.get("mapred.task.id");
	       if (taskId == null) {
	    	   throw new IllegalArgumentException("Configutaion does not contain the property mapred.task.id");
	       }

	       String[] parts = taskId.split("_");
	       if (parts.length != 6 ||
	    		   !parts[0].equals("attempt") ||
	    		   (!"m".equals(parts[3]) && !"r".equals(parts[3]))) {
	    	   throw new IllegalArgumentException("TaskAttemptId string : " + taskId + " is not properly formed");
	       }

	       return Integer.parseInt(parts[4]);
	}
	
	public static int getTTNum(JobConf job) {
	    int ttnum = 0;
		try {
			JobClient jobclient = new JobClient(job);
			ClusterStatus status = jobclient.getClusterStatus();
		    ttnum = status.getTaskTrackers();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		return ttnum;
	}
}
