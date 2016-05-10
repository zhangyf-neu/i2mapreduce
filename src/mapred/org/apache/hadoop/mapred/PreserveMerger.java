package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.IFile.Reader;
import org.apache.hadoop.mapred.IFile.TrippleReader;
import org.apache.hadoop.mapred.IFile.TrippleWriter;
import org.apache.hadoop.mapred.IFile.Writer;
import org.apache.hadoop.util.PriorityQueue;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.Progressable;

class PreserveMerger {  
  private static final Log LOG = LogFactory.getLog(PreserveMerger.class);

  // Local directories
  private static LocalDirAllocator lDirAlloc = 
    new LocalDirAllocator("mapred.local.dir");

  public static <K extends Object, V extends Object, SK extends Object>
  RawKeyValueSourceIterator merge(Configuration conf, FileSystem fs,
                            Class<K> keyClass, Class<V> valueClass, Class<SK> skeyClass,
                            CompressionCodec codec,
                            Path[] inputs, boolean deleteInputs, 
                            int mergeFactor, Path tmpDir,
                            RawComparator<K> comparator, RawComparator<SK> comparator2, Progressable reporter,
                            Counters.Counter readsCounter,
                            Counters.Counter writesCounter)
  throws IOException {
    return 
      new KVSMergeQueue<K, V, SK>(conf, fs, inputs, deleteInputs, codec, comparator, comparator2,
                           reporter).merge(keyClass, valueClass, skeyClass,
                                           mergeFactor, tmpDir,
                                           readsCounter, writesCounter);
  }
  
  public static <K extends Object, V extends Object, SK extends Object>
  RawKeyValueSourceIterator merge(Configuration conf, FileSystem fs,
                                   Class<K> keyClass, Class<V> valueClass, Class<SK> skeyClass,
                                   CompressionCodec codec,
                                   List<KVSSegment<K, V, SK>> segments, 
                                   int mergeFactor, Path tmpDir,
                                   RawComparator<K> comparator, RawComparator<SK> comparator2, Progressable reporter,
                                   Counters.Counter readsCounter,
                                   Counters.Counter writesCounter)
      throws IOException {
    return new KVSMergeQueue<K, V, SK>(conf, fs, segments, comparator, comparator2, reporter,
        false, codec).merge(keyClass, valueClass, skeyClass,
            mergeFactor, tmpDir,
            readsCounter, writesCounter);

  }

  public static <K extends Object, V extends Object, SK extends Object>
  RawKeyValueSourceIterator merge(Configuration conf, FileSystem fs, 
                            Class<K> keyClass, Class<V> valueClass, Class<SK> skeyClass, 
                            List<KVSSegment<K, V, SK>> segments,
                            int mergeFactor, Path tmpDir,
                            RawComparator<K> comparator, RawComparator<SK> comparator2, Progressable reporter,
                            Counters.Counter readsCounter,
                            Counters.Counter writesCounter)
      throws IOException {
    return merge(conf, fs, keyClass, valueClass, skeyClass, segments, mergeFactor, tmpDir,
                 comparator, comparator2, reporter, false, readsCounter, writesCounter);
  }

  public static <K extends Object, V extends Object, SK extends Object>
  RawKeyValueSourceIterator merge(Configuration conf, FileSystem fs,
                            Class<K> keyClass, Class<V> valueClass, Class<SK> skeyClass,
                            List<KVSSegment<K, V, SK>> segments,
                            int mergeFactor, Path tmpDir,
                            RawComparator<K> comparator, RawComparator<SK> comparator2, Progressable reporter,
                            boolean sortSegments,
                            Counters.Counter readsCounter,
                            Counters.Counter writesCounter)
      throws IOException {
    return new KVSMergeQueue<K, V, SK>(conf, fs, segments, comparator, comparator2, reporter,
                           sortSegments).merge(keyClass, valueClass, skeyClass,
                                               mergeFactor, tmpDir,
                                               readsCounter, writesCounter);
  }

  static <K extends Object, V extends Object,  SK extends Object>
  RawKeyValueSourceIterator merge(Configuration conf, FileSystem fs,
                            Class<K> keyClass, Class<V> valueClass, Class<SK> skeyClass,
                            List<KVSSegment<K, V, SK>> segments,
                            int mergeFactor, int inMemSegments, Path tmpDir,
                            RawComparator<K> comparator, RawComparator<SK> comparator2, Progressable reporter,
                            boolean sortSegments,
                            Counters.Counter readsCounter,
                            Counters.Counter writesCounter)
      throws IOException {
    return new KVSMergeQueue<K, V, SK>(conf, fs, segments, comparator, comparator2, reporter,
                           sortSegments).merge(keyClass, valueClass, skeyClass,
                                               mergeFactor, inMemSegments,
                                               tmpDir,
                                               readsCounter, writesCounter);
  }


  static <K extends Object, V extends Object, SK extends Object>
  RawKeyValueSourceIterator merge(Configuration conf, FileSystem fs,
                          Class<K> keyClass, Class<V> valueClass, Class<SK> skeyClass,
                          CompressionCodec codec,
                          List<KVSSegment<K, V, SK>> segments,
                          int mergeFactor, int inMemSegments, Path tmpDir,
                          RawComparator<K> comparator, RawComparator<SK> comparator2, Progressable reporter,
                          boolean sortSegments,
                          Counters.Counter readsCounter,
                          Counters.Counter writesCounter)
    throws IOException {
  return new KVSMergeQueue<K, V, SK>(conf, fs, segments, comparator, comparator2, reporter,
                         sortSegments, codec).merge(keyClass, valueClass, skeyClass,
                                             mergeFactor, inMemSegments,
                                             tmpDir,
                                             readsCounter, writesCounter);
  }

  public static <K extends Object, V extends Object ,SK extends Object>
  void writeFile(RawKeyValueSourceIterator records, TrippleWriter<K, V, SK> writer, 
                 Progressable progressable, Configuration conf) 
  throws IOException {
    long progressBar = conf.getLong("mapred.merge.recordsBeforeProgress",
        10000);
    long recordCtr = 0;
    while(records.next()) {
      writer.append(records.getKey(), records.getValue(), records.getSKey());
      //LOG.info("preserve: " + records.getKey() + "\t" + records.getValue() + "\t" + records.getSKey());
      if (((recordCtr++) % progressBar) == 0) {
        progressable.progress();
      }
    }
  }

  public static class KVSSegment<K extends Object, V extends Object, SK extends Object> {
    TrippleReader<K, V, SK> reader = null;
    DataInputBuffer key = new DataInputBuffer();
    DataInputBuffer value = new DataInputBuffer();
    DataInputBuffer skey = new DataInputBuffer();
    int priority;
    
    Configuration conf = null;
    FileSystem fs = null;
    Path file = null;
    boolean preserve = false;
    CompressionCodec codec = null;
    long segmentOffset = 0;
    long segmentLength = -1;
    
    public KVSSegment(Configuration conf, FileSystem fs, Path file,
                   CompressionCodec codec, boolean preserve, int priority) throws IOException {
      this(conf, fs, file, 0, fs.getFileStatus(file).getLen(), codec, preserve, priority);
    }

    public KVSSegment(Configuration conf, FileSystem fs, Path file,
        long segmentOffset, long segmentLength, CompressionCodec codec,
        boolean preserve, int priority) throws IOException {
      this.conf = conf;
      this.fs = fs;
      this.file = file;
      this.codec = codec;
      this.preserve = preserve;

      this.segmentOffset = segmentOffset;
      this.segmentLength = segmentLength;
      
      this.priority = priority;
    }
    
    public KVSSegment(TrippleReader<K, V, SK> reader, boolean preserve, int priority) {
      this.reader = reader;
      this.preserve = preserve;
      
      this.segmentLength = reader.getLength();
      
      this.priority = priority;
    }

    private void init(Counters.Counter readsCounter) throws IOException {
      if (reader == null) {
        FSDataInputStream in = fs.open(file);
        in.seek(segmentOffset);
        reader = new TrippleReader<K, V, SK>(conf, in, segmentLength, codec, readsCounter);
      }
    }
    
    DataInputBuffer getKey() { return key; }
    DataInputBuffer getValue() { return value; }
    DataInputBuffer getSKey() { return skey; }
    int getPriority() {return priority;}

    long getLength() { 
      return (reader == null) ?
        segmentLength : reader.getLength();
    }
    
    boolean next() throws IOException {
      return reader.next(key, value, skey);
    }
    
    void close() throws IOException {
      reader.close();
      
      if (!preserve && fs != null) {
        fs.delete(file, false);
      }
    }

    public long getPosition() throws IOException {
      return reader.getPosition();
    }
  }
  
  private static class KVSMergeQueue<K extends Object, V extends Object, SK extends Object> 
  extends PriorityQueue<KVSSegment<K, V, SK>> implements RawKeyValueSourceIterator {
    Configuration conf;
    FileSystem fs;
    CompressionCodec codec;
    
    List<KVSSegment<K, V, SK>> segments = new ArrayList<KVSSegment<K, V, SK>>();
    
    RawComparator<K> comparator;
    RawComparator<SK> comparator2;
    
    private long totalBytesProcessed;
    private float progPerByte;
    private Progress mergeProgress = new Progress();
    
    Progressable reporter;
    
    DataInputBuffer key;
    DataInputBuffer value;
    DataInputBuffer skey;
    
    KVSSegment<K, V, SK> minSegment;
    Comparator<KVSSegment<K, V, SK>> segmentComparator =   
      new Comparator<KVSSegment<K, V, SK>>() {
      public int compare(KVSSegment<K, V, SK> o1, KVSSegment<K, V, SK> o2) {
        if (o1.getLength() == o2.getLength()) {
          return 0;
        }

        return o1.getLength() < o2.getLength() ? -1 : 1;
      }
    };

    
    public KVSMergeQueue(Configuration conf, FileSystem fs, 
                      Path[] inputs, boolean deleteInputs, 
                      CompressionCodec codec, RawComparator<K> comparator, RawComparator<SK> comparator2,
                      Progressable reporter) 
    throws IOException {
      this.conf = conf;
      this.fs = fs;
      this.codec = codec;
      this.comparator = comparator;
      this.reporter = reporter;
      
      for (Path file : inputs) {
        segments.add(new KVSSegment<K, V, SK>(conf, fs, file, codec, !deleteInputs, -1));
      }
      
      // Sort segments on file-lengths
      Collections.sort(segments, segmentComparator); 
    }
    
    public KVSMergeQueue(Configuration conf, FileSystem fs,
        List<KVSSegment<K, V, SK>> segments, RawComparator<K> comparator, RawComparator<SK> comparator2,
        Progressable reporter) {
      this(conf, fs, segments, comparator, comparator2, reporter, false);
    }

    public KVSMergeQueue(Configuration conf, FileSystem fs, 
        List<KVSSegment<K, V, SK>> segments, RawComparator<K> comparator, RawComparator<SK> comparator2,
        Progressable reporter, boolean sortSegments) {
      this.conf = conf;
      this.fs = fs;
      this.comparator = comparator;
      this.comparator2 = comparator2;
      this.segments = segments;
      this.reporter = reporter;
      if (sortSegments) {
        Collections.sort(segments, segmentComparator);
      }
    }

    public KVSMergeQueue(Configuration conf, FileSystem fs,
        List<KVSSegment<K, V, SK>> segments, RawComparator<K> comparator, RawComparator<SK> comparator2,
        Progressable reporter, boolean sortSegments, CompressionCodec codec) {
      this(conf, fs, segments, comparator, comparator2, reporter, sortSegments);
      this.codec = codec;
    }

    public void close() throws IOException {
    	KVSSegment<K, V, SK> segment;
      while((segment = pop()) != null) {
        segment.close();
      }
    }

    public DataInputBuffer getKey() throws IOException {
      return key;
    }

    public DataInputBuffer getValue() throws IOException {
      return value;
    }
    
    public DataInputBuffer getSKey() throws IOException {
	    return skey;
	  }
    
    public int getPriority() {
    	return minSegment.getPriority();
    }

    private void adjustPriorityQueue(KVSSegment<K, V, SK> reader) throws IOException{
      long startPos = reader.getPosition();
      boolean hasNext = reader.next();
      long endPos = reader.getPosition();
      
      //LOG.info("adjustpriorityqueue position " + endPos + " segment " + reader.priority + " file " + reader.file);
      
      totalBytesProcessed += endPos - startPos;
      mergeProgress.set(totalBytesProcessed * progPerByte);
      if (hasNext) {
        adjustTop();
      } else {
        pop();
        reader.close();
      }
    }

    public boolean next() throws IOException {
      if (size() == 0){
        return false;
      }

      if (minSegment != null) {
        //minSegment is non-null for all invocations of next except the first
        //one. For the first invocation, the priority queue is ready for use
        //but for the subsequent invocations, first adjust the queue 
        adjustPriorityQueue(minSegment);
        if (size() == 0) {
          minSegment = null;
          return false;
        }
      }
      minSegment = top();
      //LOG.info("top is " + minSegment.priority + " file " + minSegment.file + "\t" + minSegment.getKey() + "\t" + minSegment.getSKey() + "\t" + minSegment.getValue());
      
      key = minSegment.getKey();
      value = minSegment.getValue();
      skey = minSegment.getSKey();

      return true;
    }

    public static long readLong(byte[] bytes, int start) {
        return ((long)(readInt(bytes, start)) << 32) +
          (readInt(bytes, start+4) & 0xFFFFFFFFL);
      }
    
    public static int readInt(byte[] bytes, int start) {
        return (((bytes[start  ] & 0xff) << 24) +
                ((bytes[start+1] & 0xff) << 16) +
                ((bytes[start+2] & 0xff) <<  8) +
                ((bytes[start+3] & 0xff)));

      }
    
    @SuppressWarnings("unchecked")
    protected boolean lessThan(Object ina, Object inb) {
      KVSSegment<K, V, SK> a = (KVSSegment<K, V, SK>)ina;
      KVSSegment<K, V, SK> b = (KVSSegment<K, V, SK>)inb;
      
      DataInputBuffer key1 = a.getKey();
      DataInputBuffer key2 = b.getKey();
      int s1 = key1.getPosition();
      int l1 = key1.getLength() - s1;
      int s2 = key2.getPosition();
      int l2 = key2.getLength() - s2;
      
      DataInputBuffer skey1 = a.getSKey();
      DataInputBuffer skey2 = b.getSKey();
      int ss1 = skey1.getPosition();
      int sl1 = skey1.getLength() - ss1;
      int ss2 = skey2.getPosition();
      int sl2 = skey2.getLength() - ss2;

      int comres = comparator.compare(key1.getData(), s1, l1, key2.getData(), s2, l2);
      if(comres == 0){
    	  //long thisValue = readLong(skey1.getData(), ss1);
          //long thatValue = readLong(skey2.getData(), ss2);
    	  
    	  int res = comparator2.compare(skey1.getData(), ss1, sl1, skey2.getData(), ss2, sl2);
    	  //LOG.info("comparing " + thisValue + "\t" + thatValue + "\tresult is: " + res);
    	  
    	  if(res == 0){
    		  //the last appeared k,sk,v is returned for the same k,sk
    		  //LOG.info("comparing priority " + a.getPriority() + "\t" + b.getPriority());
    		  return a.getPriority() > b.getPriority();
    	  }else{
    		  return res < 0;
    	  }
    	  
      }else {
    	  return comres < 0;
      }
    }
    
    public RawKeyValueSourceIterator merge(Class<K> keyClass, Class<V> valueClass, Class<SK> skeyClass,
                                     int factor, Path tmpDir,
                                     Counters.Counter readsCounter,
                                     Counters.Counter writesCounter)
        throws IOException {
      return merge(keyClass, valueClass, skeyClass, factor, 0, tmpDir, readsCounter, writesCounter);
    }

    RawKeyValueSourceIterator merge(Class<K> keyClass, Class<V> valueClass, Class<SK> skeyClass,
                                     int factor, int inMem, Path tmpDir,
                                     Counters.Counter readsCounter,
                                     Counters.Counter writesCounter)
        throws IOException {
      LOG.info("Merging " + segments.size() + " sorted segments");
      
      //create the MergeStreams from the sorted map created in the constructor
      //and dump the final output to a file
      int numSegments = segments.size();
      int origFactor = factor;
      int passNo = 1;
      do {
        //get the factor for this pass of merge. We assume in-memory segments
        //are the first entries in the segment list and that the pass factor
        //doesn't apply to them
        factor = getPassFactor(factor, passNo, numSegments - inMem);
        if (1 == passNo) {
          factor += inMem;
        }
        List<KVSSegment<K, V, SK>> segmentsToMerge =
          new ArrayList<KVSSegment<K, V, SK>>();
        int segmentsConsidered = 0;
        int numSegmentsToConsider = factor;
        long startBytes = 0; // starting bytes of segments of this merge
        while (true) {
          //extract the smallest 'factor' number of segments  
          //Call cleanup on the empty segments (no key/value data)
          List<KVSSegment<K, V, SK>> mStream = 
            getSegmentDescriptors(numSegmentsToConsider);
          for (KVSSegment<K, V, SK> segment : mStream) {
            // Initialize the segment at the last possible moment;
            // this helps in ensuring we don't use buffers until we need them
            segment.init(readsCounter);
            long startPos = segment.getPosition();
            //LOG.info("read segment " + segment);
            boolean hasNext = segment.next();
            long endPos = segment.getPosition();
            startBytes += endPos - startPos;
            
            if (hasNext) {
              segmentsToMerge.add(segment);
              segmentsConsidered++;
            }
            else {
              segment.close();
              numSegments--; //we ignore this segment for the merge
            }
          }
          //if we have the desired number of segments
          //or looked at all available segments, we break
          if (segmentsConsidered == factor || 
              segments.size() == 0) {
            break;
          }
            
          numSegmentsToConsider = factor - segmentsConsidered;
        }
        
        //feed the streams to the priority queue
        initialize(segmentsToMerge.size());
        clear();
        for (KVSSegment<K, V, SK> segment : segmentsToMerge) {
          put(segment);
        }
        
        //if we have lesser number of segments remaining, then just return the
        //iterator, else do another single level merge
        if (numSegments <= factor) {
          // Reset totalBytesProcessed to track the progress of the final merge.
          // This is considered the progress of the reducePhase, the 3rd phase
          // of reduce task. Currently totalBytesProcessed is not used in sort
          // phase of reduce task(i.e. when intermediate merges happen).
          totalBytesProcessed = startBytes;
          
          //calculate the length of the remaining segments. Required for 
          //calculating the merge progress
          long totalBytes = 0;
          for (int i = 0; i < segmentsToMerge.size(); i++) {
            totalBytes += segmentsToMerge.get(i).getLength();
          }
          if (totalBytes != 0) //being paranoid
            progPerByte = 1.0f / (float)totalBytes;
          
          if (totalBytes != 0)
            mergeProgress.set(totalBytesProcessed * progPerByte);
          else
            mergeProgress.set(1.0f); // Last pass and no segments left - we're done
          
          LOG.info("Down to the last merge-pass, with " + numSegments + 
                   " segments left of total size: " + totalBytes + " bytes");
          return this;
        } else {
          LOG.info("Merging " + segmentsToMerge.size() + 
                   " intermediate segments out of a total of " + 
                   (segments.size()+segmentsToMerge.size()));
          
          //we want to spread the creation of temp files on multiple disks if 
          //available under the space constraints
          long approxOutputSize = 0; 
          for (KVSSegment<K, V, SK> s : segmentsToMerge) {
            approxOutputSize += s.getLength() + 
                                ChecksumFileSystem.getApproxChkSumLength(
                                s.getLength());
          }
          Path tmpFilename = 
            new Path(tmpDir, "intermediatepreserve").suffix("." + passNo);

          Path outputFile =  lDirAlloc.getLocalPathForWrite(
                                              tmpFilename.toString(),
                                              approxOutputSize, conf);

          TrippleWriter<K, V, SK> writer = 
            new TrippleWriter<K, V, SK>(conf, fs, outputFile, keyClass, valueClass, skeyClass, codec,
                             writesCounter);
          writeFile(this, writer, reporter, conf);
          writer.close();
          
          //we finished one single level merge; now clean up the priority 
          //queue
          this.close();

          // Add the newly create segment to the list of segments to be merged
          KVSSegment<K, V, SK> tempSegment = 
            new KVSSegment<K, V, SK>(conf, fs, outputFile, codec, false , -1);
          segments.add(tempSegment);
          numSegments = segments.size();
          Collections.sort(segments, segmentComparator);
          
          passNo++;
        }
        //we are worried about only the first pass merge factor. So reset the 
        //factor to what it originally was
        factor = origFactor;
      } while(true);
    }
    
    /**
     * Determine the number of segments to merge in a given pass. Assuming more
     * than factor segments, the first pass should attempt to bring the total
     * number of segments - 1 to be divisible by the factor - 1 (each pass
     * takes X segments and produces 1) to minimize the number of merges.
     */
    private int getPassFactor(int factor, int passNo, int numSegments) {
      if (passNo > 1 || numSegments <= factor || factor == 1) 
        return factor;
      int mod = (numSegments - 1) % (factor - 1);
      if (mod == 0)
        return factor;
      return mod + 1;
    }
    
    /** Return (& remove) the requested number of segment descriptors from the
     * sorted map.
     */
    private List<KVSSegment<K, V, SK>> getSegmentDescriptors(int numDescriptors) {
      if (numDescriptors > segments.size()) {
        List<KVSSegment<K, V, SK>> subList = new ArrayList<KVSSegment<K, V, SK>>(segments);
        segments.clear();
        return subList;
      }
      
      List<KVSSegment<K, V, SK>> subList = 
        new ArrayList<KVSSegment<K, V, SK>>(segments.subList(0, numDescriptors));
      for (int i=0; i < numDescriptors; ++i) {
        segments.remove(0);
      }
      return subList;
    }

    public Progress getProgress() {
      return mergeProgress;
    }

  }
}
