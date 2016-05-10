package test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableUtils;

public class WriteData {

	public static void main(String[] args) throws IOException {

		Path p=new Path( "/home/wangq/out/datafile");
		Path p2=new Path( "/home/wangq/out/oldindex");
		FileSystem fs = FileSystem.getLocal(new Configuration());
		FSDataOutputStream datafile=fs.create(p);
		FSDataOutputStream oldindex=fs.create(p2);
		BufferedReader read=new BufferedReader(new FileReader("/home/wangq/out/part0"));
		String line,line2,line3;
 
		long off=0;
		int i=0;
		while(((line=read.readLine())!=null)&&((line2=read.readLine())!=null)&&((line3=read.readLine())!=null) ){
			
			off= datafile.getPos();
			WritableUtils.writeVInt(datafile, line.length());
			WritableUtils.writeVInt(datafile, line2.length());
			WritableUtils.writeVInt(datafile, line3.length());
 
			datafile.write(line.getBytes(), 0, line.length());
			datafile.write(line2.getBytes(), 0, line2.length());
			datafile.write(line3.getBytes(), 0, line3.length());
	 
//			System.out.println(line);
//			System.out.println(line2);
//			System.out.println(line3);
			i++;
			oldindex.writeInt((new Integer(i)).hashCode() ); 
//			System.out.println(i +"  "+(new Integer(i )).hashCode());
			oldindex.writeInt((int)off); 
			oldindex.writeInt((int)(datafile.getPos()-off) );
			 
		}
//		while( i++<40000000){
//			off= datafile.getPos();
//			
////			WritableUtils.writeVInt(datafile, WritableUtils.getVIntSize(i));
////			WritableUtils.writeVInt(datafile, WritableUtils.getVIntSize(i+1));
////			WritableUtils.writeVInt(datafile, WritableUtils.getVIntSize(i+2));
////			 
////			WritableUtils.writeVInt(datafile, i);
////			WritableUtils.writeVInt(datafile, i+1);
////			WritableUtils.writeVInt(datafile, i+2);
//	 
//			WritableUtils.writeVInt(datafile, 4);
//			WritableUtils.writeVInt(datafile, 4);
//			WritableUtils.writeVInt(datafile, 4);
//			 
//			datafile.writeInt(i);
//			datafile.writeInt(i+1);
//			datafile.writeInt(i+2);
//			 
//
//			oldindex.writeInt((new Integer(i)).hashCode() ); 
//			oldindex.writeInt((int)off) ); 
//			oldindex.writeInt((int)(datafile.getPos()-off) );
//		 
//			  
//		}
		read.close();
		oldindex.close();
		datafile.close();
		
 
	}

}
