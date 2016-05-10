/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.examples;

import org.apache.hadoop.examples.dancing.DistributedPentomino;
import org.apache.hadoop.examples.dancing.Sudoku;
import org.apache.hadoop.examples.incremental.CompSeqFile;
import org.apache.hadoop.examples.incremental.IncrApriori;
import org.apache.hadoop.examples.incremental.IncrGIMV;
import org.apache.hadoop.examples.incremental.IncrPageRank;
import org.apache.hadoop.examples.incremental.IncrSSSP;
import org.apache.hadoop.examples.incremental.UpdateGIMVData;
import org.apache.hadoop.examples.incremental.UpdateKmeansData;
import org.apache.hadoop.examples.incremental.UpdatePageRankGraph;
import org.apache.hadoop.examples.incremental.UpdateSSSPGraph;
import org.apache.hadoop.examples.iterative.IterGIMV;
import org.apache.hadoop.examples.iterative.IterKmeans;
import org.apache.hadoop.examples.iterative.IterKmeans_BigCross;
import org.apache.hadoop.examples.iterative.IterKmeans_Lastfm;
import org.apache.hadoop.examples.iterative.IterPageRank;
import org.apache.hadoop.examples.iterative.IterSSSP;
import org.apache.hadoop.examples.iterative.PreProcess;
import org.apache.hadoop.examples.naive.NaiveApriori;
import org.apache.hadoop.examples.naive.APriori;
import org.apache.hadoop.examples.naive.NaiveGIMV;
import org.apache.hadoop.examples.naive.NaiveKmeans;
import org.apache.hadoop.examples.naive.NaiveKmeans_BigCross;
import org.apache.hadoop.examples.naive.NaiveKmeans_Lastfm;
import org.apache.hadoop.examples.naive.NaivePageRank;
import org.apache.hadoop.examples.naive.NaiveSSSP;
import org.apache.hadoop.examples.naive.Test;
import org.apache.hadoop.examples.terasort.TeraGen;
import org.apache.hadoop.examples.terasort.TeraSort;
import org.apache.hadoop.examples.terasort.TeraValidate;
import org.apache.hadoop.examples.utils.FormatGraph;
import org.apache.hadoop.examples.utils.TransposeGraph;
import org.apache.hadoop.examples.utils.TrimDimsForKM;
import org.apache.hadoop.examples.utils.genGraph;
import org.apache.hadoop.examples.utils.readSeq;
import org.apache.hadoop.util.ProgramDriver;

/**
 * A description of an example program based on its class and a 
 * human-readable description.
 */
public class ExampleDriver {
  
  public static void main(String argv[]){
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    
    try {
      pgd.addClass("wordcount", WordCount.class, 
                   "A map/reduce program that counts the words in the input files.");
      pgd.addClass("aggregatewordcount", AggregateWordCount.class, 
                   "An Aggregate based map/reduce program that counts the words in the input files.");
      pgd.addClass("aggregatewordhist", AggregateWordHistogram.class, 
                   "An Aggregate based map/reduce program that computes the histogram of the words in the input files.");
      pgd.addClass("grep", Grep.class, 
                   "A map/reduce program that counts the matches of a regex in the input.");
      pgd.addClass("randomwriter", RandomWriter.class, 
                   "A map/reduce program that writes 10GB of random data per node.");
      pgd.addClass("randomtextwriter", RandomTextWriter.class, 
      "A map/reduce program that writes 10GB of random textual data per node.");
      pgd.addClass("sort", Sort.class, "A map/reduce program that sorts the data written by the random writer.");
      pgd.addClass("pi", PiEstimator.class, "A map/reduce program that estimates Pi using monte-carlo method.");
      pgd.addClass("pentomino", DistributedPentomino.class,
      "A map/reduce tile laying program to find solutions to pentomino problems.");
      pgd.addClass("secondarysort", SecondarySort.class,
                   "An example defining a secondary sort to the reduce.");
      pgd.addClass("sudoku", Sudoku.class, "A sudoku solver.");
      pgd.addClass("sleep", SleepJob.class, "A job that sleeps at each map and reduce task.");
      pgd.addClass("join", Join.class, "A job that effects a join over sorted, equally partitioned datasets");
      pgd.addClass("multifilewc", MultiFileWordCount.class, "A job that counts words from several files.");
      pgd.addClass("dbcount", DBCountPageView.class, "An example job that count the pageview counts from a database.");
      pgd.addClass("teragen", TeraGen.class, "Generate data for the terasort");
      pgd.addClass("terasort", TeraSort.class, "Run the terasort");
      pgd.addClass("teravalidate", TeraValidate.class, "Checking results of terasort");
      
      pgd.addClass("preprocess", PreProcess.class, "preprocess");
      
      //utils
      pgd.addClass("disgengraph", genGraph.class, "disgen");
      pgd.addClass("trimdims", TrimDimsForKM.class, "trimdims");
      pgd.addClass("transgraph", TransposeGraph.class, "tranpose graph");
      pgd.addClass("formatgraph", FormatGraph.class, "format graph");
      
      //for native iterative computation
      pgd.addClass("naivepagerank", NaivePageRank.class, "naive pagerank");
      pgd.addClass("naivekmeans", NaiveKmeans.class, "naive kmeans");
      pgd.addClass("naivekmeans_lastfm", NaiveKmeans_Lastfm.class, "naive kmeans for lastfm raw data");
      pgd.addClass("naivekmeans_bigcross", NaiveKmeans_BigCross.class, "naive kmeans for bigcross raw data");
      pgd.addClass("naivegimv", NaiveGIMV.class, "naive GIM-V");
      pgd.addClass("naivesssp", NaiveSSSP.class, "naive SSSP");
      
      pgd.addClass("naiveapri", APriori.class, "naive APriori");
      pgd.addClass("compapri", NaiveApriori.class, "naive APriori");
      
      //for iterative jobs
      pgd.addClass("iterpagerank", IterPageRank.class, "iterative pagerank");
      pgd.addClass("iterkmeans", IterKmeans.class, "iterative kmeans");
      pgd.addClass("iterkmeans_lastfm", IterKmeans_Lastfm.class, "iterative kmeans for lastfm raw data");
      pgd.addClass("iterkmeans_bigcross", IterKmeans_BigCross.class, "iterative kmeans for bigcross raw data");
      pgd.addClass("itergimv", IterGIMV.class, "iterative GIM-V");
      pgd.addClass("itersssp", IterSSSP.class, "iterative SSSP");

      //update the dataset
      pgd.addClass("genprupdate", UpdatePageRankGraph.class, "generate pagerank graph update");
      pgd.addClass("genkmupdate", UpdateKmeansData.class, "generate kmeans data update");
      pgd.addClass("gengimvupdate", UpdateGIMVData.class, "generate gimv data update");
      pgd.addClass("genssspupdate", UpdateSSSPGraph.class, "generate sssp graph update");
      
      //incremental processing
      pgd.addClass("incrpagerank", IncrPageRank.class, "incremental pagerank");
      pgd.addClass("incrgimv", IncrGIMV.class, "incremental GIMV");
      pgd.addClass("incrapri", IncrApriori.class, "incremental APriori");
      pgd.addClass("incrsssp", IncrSSSP.class, "incremental SSSP");
      
      pgd.addClass("compseqfile", CompSeqFile.class, "for comparing two seq files");
      pgd.addClass("readseq", readSeq.class, "read sequence file");
      pgd.addClass("test", Test.class, "test");
      
      pgd.driver(argv);
 
      // Success
      exitCode = 0;
    }
    catch(Throwable e){
      e.printStackTrace();
    }
    
    System.exit(exitCode);
  }
}
	
