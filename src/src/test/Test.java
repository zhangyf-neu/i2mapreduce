package test;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;

import preserveFile.PreserveFile;

public class Test{

	public static void main(String[] args) throws IOException {
		
		 PreserveFile<IntWritable, Text, Text, Text> file=new PreserveFile<IntWritable, Text, Text, Text>(new Configuration(),
				 new Path("/home/wangq/out/datafile"),  new Path( "/home/wangq/out/oldindex"), new Path("/home/wangq/out/newindex"), 
				 IntWritable.class, Text.class, Text.class, Text.class);
  
		  
		 
		  Deserializer<Text> keyDeserializer1;
		  Deserializer<Text> valDeserializer1;
		  Deserializer<Text> skeyDeserializer1;
		  Deserializer<Text> outvalDeserializer1;
		  DataInputBuffer keyIn1 = new DataInputBuffer();
		  DataInputBuffer valueIn1 = new DataInputBuffer();
		  DataInputBuffer skeyIn1 = new DataInputBuffer();
		  DataInputBuffer outvalueIn1 = new DataInputBuffer();
		  SerializationFactory serializationFactory = new SerializationFactory(new Configuration());
	      keyDeserializer1 = serializationFactory.getDeserializer(Text.class);
	      keyDeserializer1.open(keyIn1);
	      valDeserializer1 = serializationFactory.getDeserializer(Text.class);
	      valDeserializer1.open(valueIn1);
	      skeyDeserializer1 = serializationFactory.getDeserializer(Text.class);
	      skeyDeserializer1.open(skeyIn1);
	      outvalDeserializer1 = serializationFactory.getDeserializer(Text.class);
	      outvalDeserializer1.open(outvalueIn1);
	      
	      
	       Deserializer<Text> keyDeserializer12;
			Deserializer<Text> valDeserializer12;
			Deserializer<Text> skeyDeserializer12;
			Deserializer<Text> outvalDeserializer12;
			DataInputBuffer keyIn12 = new DataInputBuffer();
			DataInputBuffer valueIn12 = new DataInputBuffer();
			DataInputBuffer skeyIn12 = new DataInputBuffer();
			DataInputBuffer outvalueIn12 = new DataInputBuffer();
				 		 
			SerializationFactory serializationFactory2 = new SerializationFactory(new Configuration());
			keyDeserializer12 = serializationFactory.getDeserializer(Text.class);
			keyDeserializer12.open(keyIn12);
			valDeserializer12 = serializationFactory.getDeserializer(Text.class);
			valDeserializer12.open(valueIn12);
			skeyDeserializer12= serializationFactory.getDeserializer(Text.class);
			skeyDeserializer12.open(skeyIn12);
			outvalDeserializer12 = serializationFactory.getDeserializer(Text.class);
			outvalDeserializer12.open(outvalueIn12);
			
			
			 int k=1;
			 int m=file.indexCache.size();
			 System.out.println(m);
		      while (k++<m) {
//		    	  if((k%10)==0){
//		    		 
//		    		  file.seek(new IntWritable(70*k), 0, new IntWritable()) ; 
//		    		  file2.seek(new IntWritable(70*k), 0, new IntWritable()) ; 
//		    	 
//		    	  }
		    if(k>m-100000){
//						file.next(keyIn1, valueIn1, skeyIn1, outvalueIn1); 
//						Text key=new Text();
//						keyDeserializer1.deserialize(key);
//						System.out.print(k+" :"+ key+"  ");
//						
//						Text value=new Text();
//						valDeserializer1.deserialize(value);
//						System.out.print(value+"  ");
//						
//						Text skey=new Text();
//						skeyDeserializer1.deserialize(skey); 
//						System.out.println(skey);
				}
//				file2.next_old(keyIn12, valueIn12, skeyIn12, outvalueIn12); 
//				Text key2=new Text();
//				keyDeserializer12.deserialize(key2);
//				System.out.print("  ||| "+" :"+ key2+"  ");
//				
//				Text value2=new Text();
//				valDeserializer12.deserialize(value2);
//				System.out.print(value2+"  ");
//				
//				Text skey2=new Text();
//				skeyDeserializer12.deserialize(skey2); 
//				System.out.println(skey2);
		      }
	
		      
		 
		      Random ran=new Random(); 
 		      PreserveFile<IntWritable, Text, Text, Text> file2=new PreserveFile<IntWritable, Text, Text, Text>(new Configuration(),
	 					 new Path("/home/wangq/out/datafile"),  new Path( "/home/wangq/out/oldindex"), new Path("/home/wangq/out/newindex"), 
	 					 IntWritable.class, Text.class, Text.class, Text.class);
 		      
 			 long  start=System.currentTimeMillis(); 
 			 System.out.println("old begin:");
 			 
 			 for (int i = 0; i < m-21; i++) {
				int j=ran.nextInt(20);
				file2.seekKey_old (new IntWritable(i+j), 0, new IntWritable(j)) ; 
				file2.next_old (keyIn12, valueIn12, skeyIn12, outvalueIn12); 
				file2.next_old (keyIn12, valueIn12, skeyIn12, outvalueIn12);  
//				file2.next_old (keyIn12, valueIn12, skeyIn12, outvalueIn12); 
//				file2.next_old (keyIn12, valueIn12, skeyIn12, outvalueIn12); 
//				file2.next_old (keyIn12, valueIn12, skeyIn12, outvalueIn12); 
//				
//				Text key2=new Text();
//				keyDeserializer12.deserialize(key2); 
//				Text value2=new Text();
//				valDeserializer12.deserialize(value2); 
//				Text skey2=new Text();
//				skeyDeserializer12.deserialize(skey2);  
//				System.out.println(i+" "+j+" "+(i+j)+" \n"+key2.getLength()+"  "+key2+" \n"+value2.getLength()+"  "+value2+ " \n"+skey2.getLength()+"  "+skey2);
//			 
 			 }
 			 System.out.println(System.currentTimeMillis()-start);
 
 
 		 
			 System.out.println("new  begin:");
		     start=System.currentTimeMillis();
 			 for (int i = 0; i < m -21; i++) {
 				int j=ran.nextInt(20);
				file.seekKey(new IntWritable(i+j), 0, new IntWritable(j));
				file.next(keyIn1, valueIn1, skeyIn1, outvalueIn1); 
				file.next(keyIn1, valueIn1, skeyIn1, outvalueIn1);  
//				System.out.println(keyIn1.getPosition()+ " "+keyIn1.getLength()+"  "+(keyIn1.getLength()-keyIn1.getPosition()));  
//				file.next(keyIn1, valueIn1, skeyIn1, outvalueIn1); 
//				file.next(keyIn1, valueIn1, skeyIn1, outvalueIn1); 
//				file.next(keyIn1, valueIn1, skeyIn1, outvalueIn1); 
// 
//				Text key=new Text();
//				keyDeserializer1.deserialize(key);
//				Text value=new Text();
//				valDeserializer1.deserialize(value);
//				Text skey=new Text();
//				skeyDeserializer1.deserialize(skey); 
//				System.out.println(i+" "+j+" "+(i+j)+" \n"+key.getLength()+"  "+key+" \n"+value.getLength()+"  "+value+ " \n"+skey);
			 }
 			 System.out.println(System.currentTimeMillis()-start);
	}

}
