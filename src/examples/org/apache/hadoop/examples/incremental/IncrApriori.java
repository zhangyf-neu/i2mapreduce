package org.apache.hadoop.examples.incremental;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.EasyReducer;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class IncrApriori {
	static class IncrMap extends MapReduceBase implements Mapper<LongWritable,Text, Text,IntWritable>{
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
				IntWritable outvalue=new IntWritable(1);
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
				
				for(String i:set){
					for(String j:set){
					   if(i.compareTo(j)>0)
							output.collect(new Text(i+"&"+j), outvalue);
						else
							output.collect(new Text(j+"&"+i), outvalue);
					}
				}
		}
		
	}
	
	static class IncrReduce extends MapReduceBase implements EasyReducer<Text, IntWritable, Text, IntWritable>{

		static int minSupport;
		@Override
		public void configure(JobConf job) {
			minSupport=Integer.parseInt(job.get("apriori.minSupport"));
			super.configure(job);
		}
		@Override
		public IntWritable incremental(IntWritable old, IntWritable incr) {
			if(old!=null){
				int value=old.get()+incr.get();
				if(value>minSupport)
					return new IntWritable(value);
				else
					return null;
			}else{
				if(incr.get()>minSupport)
					return incr;
				else
					return null;
			} 
		}

		@Override
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException { 
			int temp=0;  
            while(values.hasNext()){
            	temp+=values.next().get();
            } 
            output.collect(key, new IntWritable(temp)); 
		}
		
	}
	public static void main(String[] args) throws IOException { 
		long start=System.currentTimeMillis();
		 if(args.length!=5){  
	            System.out.println("Usage: <input><output><preservestatepath><min_support><partitions>");  
	            System.exit(1);  
	        }  
	        String input=args[0];  
	        String output=args[1]; 
	        String preservepath=args[2];
	        int minSupport=0;  
	        int partitions = Integer.parseInt(args[4]);
	        try {  
	            minSupport=Integer.parseInt(args[3]);  
	        } catch (NumberFormatException e) {   
	            minSupport=3;  
	        }  
	        JobConf job=new JobConf(IncrApriori.class); 
	        job.setMapperClass(IncrMap.class);
	        job.setEasyReducerClass(IncrReduce.class);
	        
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(IntWritable.class);  
	        job.setPreserveStatePath(preservepath);
	        FileInputFormat.setInputPaths(job, new Path(input));  
	        FileOutputFormat.setOutputPath(job, new Path(output)); 
	        job.setEasyIncremental(true);
	        job.setNumReduceTasks(partitions);
	        job.set("apriori.minSupport", String.valueOf(minSupport));
	        JobClient.runJob(job);
	        
	        long time=System.currentTimeMillis()-start;  
			 BufferedWriter out = new BufferedWriter(new FileWriter("incr.apriori.time"));
			 out.append("incr.apriori takes time : "+time/1000+"s");
			 out.close();
	}

}
