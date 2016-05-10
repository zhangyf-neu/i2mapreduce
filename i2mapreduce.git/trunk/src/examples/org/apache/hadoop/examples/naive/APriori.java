package org.apache.hadoop.examples.naive;

import java.io.BufferedReader; 
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException; 
import java.util.*;
 
import org.apache.hadoop.filecache.DistributedCache; 
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;   
public class APriori { 
	static class OneMap extends MapReduceBase implements Mapper<LongWritable,Text, Text,IntWritable>{
		static String stopwords="able about above according accordingly across actually after afterwards again against all allow allows almost "
				+ "alone along already also although always am among amongst an and another any anybody anyhow anyone anything anyway "
				+ "anyways anywhere apart appear appreciate appropriate are around as aside ask asking associated at available away awfully be "
				+ "became because become becomes becoming been before beforehand behind being believe below beside besides best better "
				+ "between beyond both brief but by came can cannot cant cause causes certain certainly changes clearly co com come comes "
				+ "concerning consequently consider considering contain containing contains corresponding could  course  currently definitely "
				+ "described despite did  different do does  doing done  down downwards during each edu eg eight either else elsewhere enough "
				+ "entirely especially et etc even ever every everybody everyone everything everywhere ex exactly example except far few fifth first "
				+ "five followed following follows for former formerly forth four from further furthermore get gets getting given gives "
				+ "go goes going gone got gotten greetings had happens hardly has have having he hello help hence her here hereafter hereby "
				+ "herein hereupon hers herself hi him himself his hither hopefully how howbeit however ie if ignored immediatein inasmuch inc "
				+ "indeed indicate indicated indicates inner insofar instead into inward is it its itself just keep keeps kept know known "
				+ "knows last lately later latter latterly least less lest let like liked likely little look looking looks ltd mainly "
				+ "many may maybe me mean meanwhile merely might more moreover most mostly much must my myself name namely nd near nearly "
				+ "necessary need needs neither never nevertheless new next nine no nobody non none noone nor normally not nothing novel now nowhere obviously "
				+ "of off often oh ok oka old on once one ones only onto or other others otherwise ought our ours ourselves out outside"
				+ " over overall own particular particularly per perhaps placed please plus possible presumably probably provides que quite qv "
				+ "rather rd rereally reasonably regarding regardless regards relatively respectively right said same saw say saying says second "
				+ "secondly see seeing seem seemed seeming seems seen self selves sensible sent serious seriously seven several shall she should  "
				+ "since six so some somebody somehow someone something sometime sometimes somewhat somewhere soon sorry specified specify specifying"
				+ " still sub such sup sure take taken tell tends th than thank thanks thanx that thats  the their theirs them themselves "
				+ "then thence there thereafter thereby therefore therein theres  thereupon these they  think third this thorough thoroughly "
				+ "those though three through throughout thru thus to together too took toward towards tried tries truly try trying twice "
				+ "two un under unfortunately unless unlikely until unto up upon us use used useful uses using usually value various very "
				+ "via viz vs want wants was  way we welcome well went were what whatever when whence whenever where whereafter whereas whereby wherein  "
				+ "whereupon wherever whether which while whither who whoever whole whom whose why will willing "
				+ "wish with within without wonder would yes yet you your yours yourself yourselves zero zt zz" ;
		static String makeWord(String word){
			char c[]=word.toCharArray();
			for (int i = 0; i < c.length-1; i++) {
				if(!((c[i]<='z')&&(c[i]>='a'))){
					return null;
				}
			}
			String newword=word;
			if(word.length()>=2){
				if(!(c[c.length-1]>='a')&&(c[c.length-1]<='z'))
					newword= word.substring(0, word.length()-1); 
			}else
				newword=null;
			return newword;
		}
		 
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text,IntWritable> output, Reporter reporter)
				throws IOException { 
				Set<String> set=new HashSet<String>();
				 String lines[]=value.toString().split("\"text\":\"" );
				 for(int i=1;i<lines.length;i++){
					 String words[]=lines[i].split(("\""))[0].split(" ");
					 for (String word : words) {
						 String str=null;
						if((str=makeWord(word))!=null){
							if(!stopwords.contains(str)) 
								set.add(str);
						}
					}
				 } 
				 for (String word : set) {
					output.collect(new Text(word), new IntWritable(1));
				}
		}
		
	}
	static class MyReduce extends MapReduceBase implements Reducer<Text,IntWritable, Text,IntWritable>{

		static int minSupport;
		@Override
		public void configure(JobConf job) {
			minSupport=Integer.parseInt(job.get("apriori.minSupport"));
			super.configure(job);
		}

		@Override
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException { 
				int temp=0;  
	            while(values.hasNext()){
	            	temp+=values.next().get();
	            }
	            if(temp>=minSupport)
	            	output.collect(key, new IntWritable(temp));  
		} 
	}
	static class TwoMap extends MapReduceBase implements Mapper<LongWritable,Text, Text,IntWritable>{
		static Set<String> oneSet=new HashSet<String>();
		static String stopwords="able about above according accordingly across actually after afterwards again against all allow allows almost "
				+ "alone along already also although always am among amongst an and another any anybody anyhow anyone anything anyway "
				+ "anyways anywhere apart appear appreciate appropriate are around as aside ask asking associated at available away awfully be "
				+ "became because become becomes becoming been before beforehand behind being believe below beside besides best better "
				+ "between beyond both brief but by came can cannot cant cause causes certain certainly changes clearly co com come comes "
				+ "concerning consequently consider considering contain containing contains corresponding could  course  currently definitely "
				+ "described despite did  different do does  doing done  down downwards during each edu eg eight either else elsewhere enough "
				+ "entirely especially et etc even ever every everybody everyone everything everywhere ex exactly example except far few fifth first "
				+ "five followed following follows for former formerly forth four from further furthermore get gets getting given gives "
				+ "go goes going gone got gotten greetings had happens hardly has have having he hello help hence her here hereafter hereby "
				+ "herein hereupon hers herself hi him himself his hither hopefully how howbeit however ie if ignored immediatein inasmuch inc "
				+ "indeed indicate indicated indicates inner insofar instead into inward is it its itself just keep keeps kept know known "
				+ "knows last lately later latter latterly least less lest let like liked likely little look looking looks ltd mainly "
				+ "many may maybe me mean meanwhile merely might more moreover most mostly much must my myself name namely nd near nearly "
				+ "necessary need needs neither never nevertheless new next nine no nobody non none noone nor normally not nothing novel now nowhere obviously "
				+ "of off often oh ok oka old on once one ones only onto or other others otherwise ought our ours ourselves out outside"
				+ " over overall own particular particularly per perhaps placed please plus possible presumably probably provides que quite qv "
				+ "rather rd rereally reasonably regarding regardless regards relatively respectively right said same saw say saying says second "
				+ "secondly see seeing seem seemed seeming seems seen self selves sensible sent serious seriously seven several shall she should  "
				+ "since six so some somebody somehow someone something sometime sometimes somewhat somewhere soon sorry specified specify specifying"
				+ " still sub such sup sure take taken tell tends th than thank thanks thanx that thats  the their theirs them themselves "
				+ "then thence there thereafter thereby therefore therein theres  thereupon these they  think third this thorough thoroughly "
				+ "those though three through throughout thru thus to together too took toward towards tried tries truly try trying twice "
				+ "two un under unfortunately unless unlikely until unto up upon us use used useful uses using usually value various very "
				+ "via viz vs want wants was  way we welcome well went were what whatever when whence whenever where whereafter whereas whereby wherein  "
				+ "whereupon wherever whether which while whither who whoever whole whom whose why will willing "
				+ "wish with within without wonder would yes yet you your yours yourself yourselves zero zt zz" ;
		static String makeWord(String word){
			char c[]=word.toCharArray();
			for (int i = 0; i < c.length-1; i++) {
				if(!((c[i]<='z')&&(c[i]>='a'))){
					return null;
				}
			}
			String newword=word;
			if(word.length()>=2){
				if(!(c[c.length-1]>='a')&&(c[c.length-1]<='z'))
					newword= word.substring(0, word.length()-1); 
			}else
				newword=null;
			return newword;
		}
		 
		@Override
		public void configure(JobConf job) {  
			Path [] pathsFiles=new Path[0]; 
			try {
				pathsFiles=DistributedCache.getLocalCacheFiles(job); 
			} catch (IOException e) { 
				e.printStackTrace();
			}
			for (Path pathsFile : pathsFiles) { 
				try {
					BufferedReader fis = new BufferedReader(new FileReader(pathsFile.toString()));
					String line;
					while((line=fis.readLine())!=null){ 
						oneSet.add(line.split("\t")[0]);
					}
					fis.close();
				} catch (FileNotFoundException e) { 
					e.printStackTrace();
				} catch (IOException e) { 
					e.printStackTrace(); 
				}
			}
			super.configure(job);
		}

		@Override
		public void map(LongWritable key, Text value, 
				OutputCollector<Text,IntWritable> output, Reporter reporter)
				throws IOException {     
			  	Set<String> set=new HashSet<String>();
				 String lines[]=value.toString().split("\"text\":\"" );
				 for(int i=1;i<lines.length;i++){
					 String words[]=lines[i].split(("\""))[0].split(" ");
					 for (String word : words) {
						 String str=null;
						if((str=makeWord(word))!=null){
							if(!stopwords.contains(str)) 
								set.add(str);
						}
					}
				 } 
	            Iterator<String> iter=oneSet.iterator();  
	            StringBuffer sb=new StringBuffer();  
	            sb.setLength(0);  
	            int num=0;   
	            while(iter.hasNext()){   
	                String item=iter.next();  
	                if(set.contains(item)){  
	                    sb.append(item+",");  
	                    num++;  
	                }  
	            }   
	         
	            if(num>1){
	            	String [] items=sb.toString().split(",");
	            	for(int i=0;i<items.length-1;i++){
	            		for(int j=i+1;j<items.length;j++){
	            			if(items[i].compareTo(items[j])>0)
	            				output.collect(new Text(items[i]+"&"+items[j]), new IntWritable(1));
	            			else
	            				output.collect(new Text(items[j]+"&"+items[i]), new IntWritable(1));
	            		}
	            	}
	            }
		}
	}
 
	
	public static void main(String[] args) throws IOException { 
		 // TODO Auto-generated method stub   
        if(args.length!=4){  
            System.out.println("Usage: <input><output><min_support><partitions>");  
            System.exit(1);  
        }  
        String input=args[0];  
        String output=args[1];  
        int minSupport=0;  
        int partitions = Integer.parseInt(args[3]);
        
        try {  
            minSupport=Integer.parseInt(args[2]);  
        } catch (NumberFormatException e) {   
            minSupport=3;  
        }  
         
        String one=output+"/one"; 
        
        JobConf job1=new JobConf(APriori.class);   
        job1.setMapperClass(OneMap.class);   
        job1.setReducerClass(MyReduce.class);  
        job1.setOutputKeyClass(Text.class);  
        job1.setOutputValueClass(IntWritable.class);  
        job1.set("apriori.minSupport", String.valueOf(minSupport));
        FileInputFormat.setInputPaths(job1, new Path(input));  
        FileOutputFormat.setOutputPath(job1, new Path(one));  
        job1.setEasyPreserve(true); 
        job1.setNumReduceTasks(partitions);
        JobClient.runJob(job1);            
                 
        
        String two=output+"/two"; 
        JobConf job2=new JobConf(APriori.class);   
        Path path=new Path(one);
        FileSystem fs =path.getFileSystem(job2);
        if(fs.isDirectory(path)){ 
        	for (FileStatus fileStatus : fs.listStatus(path)) {
        		 if(!fileStatus.isDir())
        			 DistributedCache.addCacheFile(fileStatus.getPath().toUri(),job2);
			}
        } 
        job2.setMapperClass(TwoMap.class);   
        job2.setReducerClass(MyReduce.class);  
        job2.setOutputKeyClass(Text.class);  
        job2.setOutputValueClass(IntWritable.class);  
        job2.set("apriori.minSupport", String.valueOf(minSupport));
        FileInputFormat.setInputPaths(job2, new Path(input));  
        FileOutputFormat.setOutputPath(job2, new Path(two));  
        job2.setEasyPreserve(true);  
        job2.setNumReduceTasks(partitions);
        JobClient.runJob(job2); 
	}
	
}
