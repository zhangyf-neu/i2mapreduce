package preserveFile;

import java.io.DataOutput;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
 
import org.apache.hadoop.mapred.io.BufferedRandomAccessFile;

 
 public   class PreserveFile<KEY extends Object, VALUE extends Object, SOURCE extends Object, OUTVALUE extends Object>  {
    private static final int DEFAULT_BUFFER_SIZE = 128*1024;
    private static final int MAX_VINT_SIZE = 3*9;
    private static final int INDEX_ENTRY_SIZE = 12;
    private static final int RESULT_KV_LABEL = -100;
    private static final int EOF_MARKER = -1;
    public enum TYPE{FILEEND, RECORDEND, MORE}
    
    class IndexEntry{
      	int offset;
      	int length;
      	
      	public IndexEntry(int offset, int length){
      		this.offset = offset;
      		this.length = length;
      	}
      	
      	@Override
      	public String toString(){
      		return "index entry: " + offset + "\t" + length;
      	}
    }
    
    // Count records read from disk
    private long numRecordsRead = 0;

    BufferedRandomAccessFile  dataFile;
    BufferedRandomAccessFile appendFile;
    FSDataInputStream indexIns;
    FSDataOutputStream indexOut;
    
    long readbytes = 0;
    final long fileLength;
    boolean eof = false;
    boolean indexend = false;
    
    byte[] databuffer = null;
    byte[] indexbuffer = null;
    int bufferSize = DEFAULT_BUFFER_SIZE;
    int indexbufferSize = DEFAULT_BUFFER_SIZE;
    DataInputBuffer dataIn = new DataInputBuffer();
    DataInputBuffer indexIn = new DataInputBuffer();
	    
    int bytesWritten = 0;

    // Count records written to disk
    private long numRecordsWritten = 0;
    
    //<hashcode, <offset, length>>
     public TreeMap<Integer, IndexEntry> indexCache = new TreeMap<Integer, IndexEntry>();

    Class<KEY> t1Class;
    Class<VALUE> t2Class;
    Class<SOURCE> t3Class;
    Class<OUTVALUE> t4Class;
    Serializer<KEY> t1Serializer;
    Serializer<VALUE> t2Serializer;
    Serializer<SOURCE> t3Serializer;
    Serializer<OUTVALUE> t4Serializer;
    
    DataOutputBuffer buffer = new DataOutputBuffer();
    
    KEY cachedKey;
    int cachedOffset;
    int recNo = 1;

    public PreserveFile(Configuration conf, Path datafile, Path oldindexfile, Path newindexfile,
    				Class<KEY> t1Class, Class<VALUE> t2Class, Class<SOURCE> t3Class, Class<OUTVALUE> t4Class) throws IOException {
      FileSystem fs = FileSystem.getLocal(conf);
 
      this.dataFile = new BufferedRandomAccessFile(datafile.toString(), "rw");
//      System.out.println( dataFile.read());
      File tmpFile = new File(datafile.toString() + "-append.tmp");
      if(tmpFile.exists()){
    	  tmpFile.delete();
    	//  Log.info("delete file " + tmpFile);
      }
      this.appendFile = new BufferedRandomAccessFile(datafile.toString() + "-append.tmp", "rw");
  
      this.t1Class = t1Class;
      this.t2Class = t2Class;
      this.t3Class = t3Class;
      this.t4Class = t4Class;
      SerializationFactory serializationFactory = new SerializationFactory(conf);
      this.t1Serializer =  serializationFactory.getSerializer(t1Class);
      this.t1Serializer.open(buffer);
      this.t2Serializer = serializationFactory.getSerializer(t2Class);
      this.t2Serializer.open(buffer);
      this.t3Serializer = serializationFactory.getSerializer(t3Class);
      this.t3Serializer.open(buffer);
      this.t4Serializer = serializationFactory.getSerializer(t4Class);
      this.t4Serializer.open(buffer);
      
      indexOut = fs.create(newindexfile);
      this.fileLength = fs.getFileStatus(datafile).getLen();
      
      if (conf != null) {
        //bufferSize = conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE);
    	bufferSize = DEFAULT_BUFFER_SIZE;
      }
      
      //Log.info("read buffer size " + bufferSize);
      
      databuffer = new byte[bufferSize];
      dataIn.reset(databuffer, 0, 0);
      
      int total = 0;
      long start = System.currentTimeMillis();
      //incrmental start, no oldindexfile
      if(oldindexfile != null){
    	  indexIns = fs.open(oldindexfile);
    	  
    	  //total = indexIns.readInt();
    	  
          indexbuffer = new byte[indexbufferSize];
          indexIn.reset(indexbuffer, 0, 0);
          
    	  //load the indices
          while(true){
          	if(indexIn.getLength() - indexIn.getPosition() < INDEX_ENTRY_SIZE){
          		if(indexend) break;
          		
          		readIndexNextBlock(INDEX_ENTRY_SIZE);
          	}
          	
          	int hashcode = indexIn.readInt();
           
          	int offset = indexIn.readInt();
           
          	int length = indexIn.readInt();
//          	System.out.println(hashcode+"  "+offset+"  "+length);
          	indexCache.put(hashcode, new IndexEntry(offset, length));
          	//Log.info("put " + hashcode + "\t" + offset + "\t" + length);
          } 
          
          indexIn.close();
          indexbuffer = null;
      }
      long end = System.currentTimeMillis();
      
     // Log.info("load index file " + indexCache.size() + " entries! expected entries " + total + " use time " + (end-start) + " ms");
    }

    public void close_old() throws IOException {

      // Release the buffer
      dataIn = null;
      databuffer = null;
      
      // Close the serializers
      t1Serializer.close();
      t2Serializer.close();
      t3Serializer.close();
      t4Serializer.close();

      //append the appendfile to the datafile, merge two files as one file
      appendFile.seek(0);
      dataFile.seek(dataFile.length() - 3);
      int len;
      byte[] buf = new byte[128 * 1024];
      while ((len = appendFile.read(buf)) > 0){
    	  dataFile.write(buf, 0, len);
      }
      
      WritableUtils.writeVInt(dataFile, EOF_MARKER);
      WritableUtils.writeVInt(dataFile, EOF_MARKER);
      WritableUtils.writeVInt(dataFile, EOF_MARKER);
      
     // Log.info("append " + appendFile.length() + " bytes to the dataFile");
      
      // Close the underlying stream iff we own it...
      dataFile.close();
      dataFile = null;
      appendFile.close();
      appendFile = null;
      
  	  //write the hashcodes length
  	  indexOut.writeInt(indexCache.size());
  	
  	  //how to handle the hash conflict problem?
  	  //write the k,sk,v that with the same hashcode sequentially as their appearance
  	  int i = 0;
  	  for(Map.Entry<Integer, IndexEntry> entry : indexCache.entrySet()){
  		indexOut.writeInt(entry.getKey());
  		indexOut.writeInt(entry.getValue().offset);
  		indexOut.writeInt(entry.getValue().length);
  		i++;
  	  }
  //	  Log.info("index file write " + i + " entries!");
  	  
      indexOut.close();
      indexOut = null;
    }

    private int getHashcode(KEY t1){
        //if there is a hash conflict, then try next hashcode
        int keyhash = t1.hashCode();
        while(indexCache.containsKey(keyhash)){
        	//Log.info("hash conflict " + t1);
      	    keyhash = String.valueOf(keyhash).hashCode();
        }
        
        return keyhash;
    }
    
    public void appendShuffleKVS_old(KEY t1, VALUE t2, SOURCE t3) throws IOException {
        if (t1.getClass() != t1Class)
            throw new IOException("wrong t1 class: "+ t1.getClass()
                                  +" is not "+ t1Class);
        
      if (t2.getClass() != t2Class)
        throw new IOException("wrong t2 class: "+ t2.getClass()
                              +" is not "+ t2Class);
      if (t3.getClass() != t3Class)
        throw new IOException("wrong t3 class: "+ t3.getClass()
                              +" is not "+ t3Class);

	  if(!t1.equals(cachedKey)){
		  cachedKey = t1;
		  cachedOffset = (int)dataFile.length();
	  }
      
      // Append the 'key'
      t1Serializer.serialize(t1);
      int t1Length = buffer.getLength();
      if (t1Length < 0) {
        throw new IOException("Negative key-length not allowed: " + t1Length + 
                              " for " + t1);
      }
      
      // Append the 'key'
      t2Serializer.serialize(t2);
      int t2Length = buffer.getLength() - t1Length;
      if (t2Length < 0) {
        throw new IOException("Negative t2-length not allowed: " + t2Length + 
                              " for " + t2);
      }

      // Append the 'value'
      t3Serializer.serialize(t3);
      int t3Length = buffer.getLength() - t2Length - t1Length;
      if (t3Length < 0) {
        throw new IOException("Negative t3-length not allowed: " + 
                              t3Length + " for " + t3);
      }
      
      // Write the record out
      WritableUtils.writeVInt(dataFile, t1Length);                  // key length
      WritableUtils.writeVInt(dataFile, t2Length);                  // key length
      WritableUtils.writeVInt(dataFile, t3Length);                // value length

      /********************************
       * same key length and same value length, need optimize later
       */
      
      //Log.info("key length : " + keyLength + " value length : " + valueLength);
      dataFile.write(buffer.getData(), 0, buffer.getLength());       // data

      // Reset
      buffer.reset();
      
      // Update bytes written
      bytesWritten += t1Length + t2Length + t3Length + 
    		  					  WritableUtils.getVIntSize(t1Length) + 
                                  WritableUtils.getVIntSize(t2Length) + 
                                  WritableUtils.getVIntSize(t3Length);
      
      ++numRecordsWritten;
    }
    //appendResKV must be involked after appendShuffleKVS
    public void appendResKV_old(KEY t1, OUTVALUE t4) throws IOException {
        if (t1.getClass() != t1Class)
            throw new IOException("wrong t1 class: "+ t1.getClass()
                                  +" is not "+ t1Class);
        
      if (t4.getClass() != t4Class)
        throw new IOException("wrong t4 class: "+ t4.getClass()
                              +" is not "+ t4Class);
      
      // Append the 'key'
      t1Serializer.serialize(t1);
      int t1Length = buffer.getLength();
      if (t1Length < 0) {
        throw new IOException("Negative key-length not allowed: " + t1Length + 
                              " for " + t1);
      }
      
      // Append the 'rvalue'
      t4Serializer.serialize(t4);
      int t4Length = buffer.getLength() - t1Length;
      if (t4Length < 0) {
        throw new IOException("Negative t4-length not allowed: " + t4Length + 
                              " for " + t4);
      }
      
      // Write the record out
      WritableUtils.writeVInt(dataFile, t1Length);                  // key length
      WritableUtils.writeVInt(dataFile, t4Length);                  // key length
      WritableUtils.writeVInt(dataFile, RESULT_KV_LABEL);           // value length

      /********************************
       * same key length and same value length, need optimize later
       */
      
      //Log.info("key length : " + keyLength + " value length : " + valueLength);
      dataFile.write(buffer.getData(), 0, buffer.getLength());       // data

      // Reset
      buffer.reset();
      
      // Update bytes written
      bytesWritten += t1Length + t4Length + 
    		  					  WritableUtils.getVIntSize(t1Length) + 
                                  WritableUtils.getVIntSize(t4Length) + 
                                  WritableUtils.getVIntSize(RESULT_KV_LABEL);
      ++numRecordsWritten;
      
	  if(!t1.equals(cachedKey)){
		  throw new IOException(t1 + "is not the key " + cachedKey + " that should be!");
	  }else{
		  int hash = getHashcode(t1);
		  indexCache.put(hash, new IndexEntry(cachedOffset, bytesWritten));
		  
		  //Log.info("appned " + t1 + " hashkey " + hash + "\toffset " + cachedOffset + "\tlength " + bytesWritten);
		  
		  cachedOffset = -1;
		  bytesWritten = 0;
	  }
    }
     
    public void updateShuffleKVS_old(int keyhash, KEY t1, VALUE t2, SOURCE t3) throws IOException {
        if (t1.getClass() != t1Class)
            throw new IOException("wrong t1 class: "+ t1.getClass()
                                  +" is not "+ t1Class);
        
      if (t2.getClass() != t2Class)
        throw new IOException("wrong t2 class: "+ t2.getClass()
                              +" is not "+ t2Class);
      if (t3.getClass() != t3Class)
        throw new IOException("wrong t3 class: "+ t3.getClass()
                              +" is not "+ t3Class);

      //if not the cached key, that means t1 is a new key, we should update the index cache,
      //or else, we do nothing
	  if(!t1.equals(cachedKey)){
		  cachedKey = t1;
		  cachedOffset = (int)(appendFile.length() + dataFile.length() - 3);
		  //Log.info("put index cache " + t1 + " hashkey " + keyhash + "\toffset " + (appendFile.length() + dataFile.length() - 3));
	  }
	  
      // Append the 'key'
      t1Serializer.serialize(t1);
      int t1Length = buffer.getLength();
      if (t1Length < 0) {
        throw new IOException("Negative key-length not allowed: " + t1Length + 
                              " for " + t1);
      }
      
      // Append the 'key'
      t2Serializer.serialize(t2);
      int t2Length = buffer.getLength() - t1Length;
      if (t2Length < 0) {
        throw new IOException("Negative t2-length not allowed: " + t2Length + 
                              " for " + t2);
      }

      // Append the 'value'
      t3Serializer.serialize(t3);
      int t3Length = buffer.getLength() - t2Length - t1Length;
      if (t3Length < 0) {
        throw new IOException("Negative t3-length not allowed: " + 
                              t3Length + " for " + t3);
      }
      
      // Write the record out
//      WritableUtils.writeVInt(appendFile, t1Length);                  // key length
//      WritableUtils.writeVInt(appendFile, t2Length);                  // key length
//      WritableUtils.writeVInt(appendFile, t3Length);                // value length

      /********************************
       * same key length and same value length, need optimize later
       */
      
      //Log.info("key length : " + keyLength + " value length : " + valueLength);
      appendFile.write(buffer.getData(), 0, buffer.getLength());       // data

      // Reset
      buffer.reset();
      
      // Update bytes written
      bytesWritten += t1Length + t2Length + t3Length + 
    		  					  WritableUtils.getVIntSize(t1Length) + 
                                  WritableUtils.getVIntSize(t2Length) + 
                                  WritableUtils.getVIntSize(t3Length);
      ++numRecordsWritten;
    }
    
    public void updateResKV_old(int keyhash, KEY t1, OUTVALUE t4) throws IOException {
        if (t1.getClass() != t1Class)
            throw new IOException("wrong t1 class: "+ t1.getClass()
                                  +" is not "+ t1Class);
        
      if (t4.getClass() != t4Class)
        throw new IOException("wrong t4 class: "+ t4.getClass()
                              +" is not "+ t4Class);
	  
      // Append the 'key'
      t1Serializer.serialize(t1);
      int t1Length = buffer.getLength();
      if (t1Length < 0) {
        throw new IOException("Negative key-length not allowed: " + t1Length + 
                              " for " + t1);
      }
      
      // Append the 'outvalue'
      t4Serializer.serialize(t4);
      int t4Length = buffer.getLength() - t1Length;
      if (t4Length < 0) {
        throw new IOException("Negative t4-length not allowed: " + t4Length + 
                              " for " + t4);
      }

      // Write the record out
      WritableUtils.writeVInt(appendFile, t1Length);                  // key length
      WritableUtils.writeVInt(appendFile, t4Length);                  // key length
      WritableUtils.writeVInt(appendFile, RESULT_KV_LABEL);                // value length

      /********************************
       * same key length and same value length, need optimize later
       */
      
      //Log.info("key length : " + keyLength + " value length : " + valueLength);
      appendFile.write(buffer.getData(), 0, buffer.getLength());       // data

      // Reset
      buffer.reset();
      
      // Update bytes written
      bytesWritten += t1Length + t4Length +
    		  					  WritableUtils.getVIntSize(t1Length) + 
                                  WritableUtils.getVIntSize(t4Length) + 
                                  WritableUtils.getVIntSize(RESULT_KV_LABEL);
      ++numRecordsWritten;
      
      //updateShuffleKVS followed by updateResKV
	  if(!t1.equals(cachedKey)){
		  throw new IOException(t1 + "is not the key " + cachedKey + " that should be!");
	  }else{
		  indexCache.put(keyhash, new IndexEntry(cachedOffset, bytesWritten));
		  
		  //Log.info("update " + t1 + " hashkey " + keyhash + "\toffset " + cachedOffset + "\tlength " + bytesWritten);
		  
		  cachedOffset = -1;
		  bytesWritten = 0;
	  }
    }
    
   
    public void close() throws IOException {

        // Release the buffer
        dataIn = null;
        databuffer = null;
        
        dataFile.write(appbuf, 0, appbuf.length);
        appendFile.write(update_buf, 0, update_buf.length);
        // Close the serializers
        t1Serializer.close();
        t2Serializer.close();
        t3Serializer.close();
        t4Serializer.close();

        //append the appendfile to the datafile, merge two files as one file
        appendFile.seek(0);
        dataFile.seek(dataFile.length() - 3);
        int len;
        byte[] buf = new byte[128 * 1024];
        while ((len = appendFile.read(buf)) > 0){
      	  dataFile.write(buf, 0, len);
        }
        
        WritableUtils.writeVInt(dataFile, EOF_MARKER);
        WritableUtils.writeVInt(dataFile, EOF_MARKER);
        WritableUtils.writeVInt(dataFile, EOF_MARKER);
        
       // Log.info("append " + appendFile.length() + " bytes to the dataFile");
        
        // Close the underlying stream iff we own it...
        dataFile.close();
        dataFile = null;
        appendFile.close();
        appendFile = null;
        
    	  //write the hashcodes length
    	  indexOut.writeInt(indexCache.size());
    	
    	  //how to handle the hash conflict problem?
    	  //write the k,sk,v that with the same hashcode sequentially as their appearance
    	  int i = 0;
    	  for(Map.Entry<Integer, IndexEntry> entry : indexCache.entrySet()){
    		indexOut.writeInt(entry.getKey());
    		indexOut.writeInt(entry.getValue().offset);
    		indexOut.writeInt(entry.getValue().length);
    		i++;
    	  }
    //	  Log.info("index file write " + i + " entries!");
    	  
        indexOut.close();
        indexOut = null;
      }

   private byte appbuf[]=new byte[bufferSize];
   private long append_pos;
    public void appendShuffleKVS(KEY t1, VALUE t2, SOURCE t3) throws IOException {
        if (t1.getClass() != t1Class)
            throw new IOException("wrong t1 class: "+ t1.getClass()
                                  +" is not "+ t1Class);
        
      if (t2.getClass() != t2Class)
        throw new IOException("wrong t2 class: "+ t2.getClass()
                              +" is not "+ t2Class);
      if (t3.getClass() != t3Class)
        throw new IOException("wrong t3 class: "+ t3.getClass()
                              +" is not "+ t3Class);

	  if(!t1.equals(cachedKey)){
		  cachedKey = t1;
		  cachedOffset = (int)dataFile.length();
	  }
      
      // Append the 'key'
      t1Serializer.serialize(t1);
      int t1Length = buffer.getLength();
      if (t1Length < 0) {
        throw new IOException("Negative key-length not allowed: " + t1Length + 
                              " for " + t1);
      }
      
      // Append the 'key'
      t2Serializer.serialize(t2);
      int t2Length = buffer.getLength() - t1Length;
      if (t2Length < 0) {
        throw new IOException("Negative t2-length not allowed: " + t2Length + 
                              " for " + t2);
      }

      // Append the 'value'
      t3Serializer.serialize(t3);
      int t3Length = buffer.getLength() - t2Length - t1Length;
      if (t3Length < 0) {
        throw new IOException("Negative t3-length not allowed: " + 
                              t3Length + " for " + t3);
      }
      
      // Write the record out
      DataOutputBuffer temp=new DataOutputBuffer();
      WritableUtils.writeVInt(temp, t1Length);
      WritableUtils.writeVInt(temp, t2Length);
      WritableUtils.writeVInt(temp, t3Length);
 
      if((t1Length+t2Length+t3Length+WritableUtils.getVIntSize(t3Length)
    		  +WritableUtils.getVIntSize(t2Length)+WritableUtils.getVIntSize(t1Length)+append_pos)>=bufferSize){
    	  dataFile.write(appbuf, 0, (int)append_pos);
    	  append_pos=0;
      } 
    	  System.arraycopy(temp.getData(), 0, appbuf, (int)append_pos, temp.getData().length);
    	  append_pos+=temp.getData().length;
    	  temp.reset();
    	 
       
      
      /********************************
       * same key length and same value length, need optimize later
       */
      
      //Log.info("key length : " + keyLength + " value length : " + valueLength);
//      dataFile.write(buffer.getData(), 0, buffer.getLength());       // data
      System.arraycopy(buffer.getData(), 0, appbuf, (int)append_pos, buffer.getData().length);
      append_pos+=buffer.getData().length;
    
      // Reset
      buffer.reset();
      
      // Update bytes written
      bytesWritten += t1Length + t2Length + t3Length + 
    		  					  WritableUtils.getVIntSize(t1Length) + 
                                  WritableUtils.getVIntSize(t2Length) + 
                                  WritableUtils.getVIntSize(t3Length);
      
      ++numRecordsWritten;
    }
 
    public void appendResKV(KEY t1, OUTVALUE t4) throws IOException {
        if (t1.getClass() != t1Class)
            throw new IOException("wrong t1 class: "+ t1.getClass()
                                  +" is not "+ t1Class);
        
      if (t4.getClass() != t4Class)
        throw new IOException("wrong t4 class: "+ t4.getClass()
                              +" is not "+ t4Class);
      
      // Append the 'key'
//      t1Serializer.serialize(t1);
      int t1Length = buffer.getLength();
      if (t1Length < 0) {
        throw new IOException("Negative key-length not allowed: " + t1Length + 
                              " for " + t1);
      }
      
      // Append the 'rvalue'
//      t4Serializer.serialize(t4);
      int t4Length = buffer.getLength() - t1Length;
      if (t4Length < 0) {
        throw new IOException("Negative t4-length not allowed: " + t4Length + 
                              " for " + t4);
      }
      DataOutputBuffer temp=new DataOutputBuffer();
      WritableUtils.writeVInt(temp, t1Length);
      WritableUtils.writeVInt(temp, t4Length); 
      WritableUtils.writeVInt(temp, RESULT_KV_LABEL);
      // Write the record out
//      WritableUtils.writeVInt(dataFile, t1Length);                  // key length
//      WritableUtils.writeVInt(dataFile, t4Length);                  // key length
//      WritableUtils.writeVInt(dataFile, RESULT_KV_LABEL);           // value length

      if((t1Length+t4Length+WritableUtils.getVIntSize(t4Length)
    		  +WritableUtils.getVIntSize(RESULT_KV_LABEL)+WritableUtils.getVIntSize(t1Length)+append_pos)>=bufferSize){
    	  	dataFile.write(appbuf, 0, (int)append_pos);
    	  	append_pos=0;
      } 
    	  System.arraycopy(temp.getData(), 0, appbuf, (int)append_pos, temp.getLength());
    	  append_pos+=temp.getLength();
    	  temp.reset();
     
       
      /********************************
       * same key length and same value length, need optimize later
       */
      
      //Log.info("key length : " + keyLength + " value length : " + valueLength);
//      dataFile.write(buffer.getData(), 0, buffer.getLength());       // data
    	  System.arraycopy(buffer.getData(), 0, appbuf, (int)append_pos, buffer.getLength());
    	  append_pos+=buffer.getLength();
       
      // Reset
      buffer.reset();
      
      // Update bytes written
      bytesWritten += t1Length + t4Length + 
    		  					  WritableUtils.getVIntSize(t1Length) + 
                                  WritableUtils.getVIntSize(t4Length) + 
                                  WritableUtils.getVIntSize(RESULT_KV_LABEL);
      ++numRecordsWritten;
      
	  if(!t1.equals(cachedKey)){
		  throw new IOException(t1 + "is not the key " + cachedKey + " that should be!");
	  }else{
		  int hash = getHashcode(t1);
		  indexCache.put(hash, new IndexEntry(cachedOffset, bytesWritten));
		 
	 
		  //Log.info("appned " + t1 + " hashkey " + hash + "\toffset " + cachedOffset + "\tlength " + bytesWritten);
		  
		  cachedOffset = -1;
		  bytesWritten = 0;
	  }
    }
    
   private byte update_buf[]=new byte[bufferSize];
   private long update_pos;
   public void updateShuffleKVS(int keyhash, KEY t1, VALUE t2, SOURCE t3) throws IOException {
        if (t1.getClass() != t1Class)
            throw new IOException("wrong t1 class: "+ t1.getClass()
                                  +" is not "+ t1Class);
        
      if (t2.getClass() != t2Class)
        throw new IOException("wrong t2 class: "+ t2.getClass()
                              +" is not "+ t2Class);
      if (t3.getClass() != t3Class)
        throw new IOException("wrong t3 class: "+ t3.getClass()
                              +" is not "+ t3Class);

      //if not the cached key, that means t1 is a new key, we should update the index cache,
      //or else, we do nothing
	  if(!t1.equals(cachedKey)){
		  cachedKey = t1;
		  cachedOffset = (int)(appendFile.length() + dataFile.length() - 3);
		  //Log.info("put index cache " + t1 + " hashkey " + keyhash + "\toffset " + (appendFile.length() + dataFile.length() - 3));
	  }
	  
      // Append the 'key'
      t1Serializer.serialize(t1);
      int t1Length = buffer.getLength();
      if (t1Length < 0) {
        throw new IOException("Negative key-length not allowed: " + t1Length + 
                              " for " + t1);
      }
      
      // Append the 'key'
      t2Serializer.serialize(t2);
      int t2Length = buffer.getLength() - t1Length;
      if (t2Length < 0) {
        throw new IOException("Negative t2-length not allowed: " + t2Length + 
                              " for " + t2);
      }

      // Append the 'value'
      t3Serializer.serialize(t3);
      int t3Length = buffer.getLength() - t2Length - t1Length;
      if (t3Length < 0) {
        throw new IOException("Negative t3-length not allowed: " + 
                              t3Length + " for " + t3);
      }
      
      // Write the record out
      DataOutputBuffer temp=new DataOutputBuffer();
      WritableUtils.writeVInt(temp, t1Length);
      WritableUtils.writeVInt(temp, t2Length);
      WritableUtils.writeVInt(temp, t3Length);
//      WritableUtils.writeVInt(appendFile, t1Length);                  // key length
//      WritableUtils.writeVInt(appendFile, t2Length);                  // key length
//      WritableUtils.writeVInt(appendFile, t3Length);                // value length

      if((t1Length+t2Length+t3Length+WritableUtils.getVIntSize(t3Length)
    		  +WritableUtils.getVIntSize(t2Length)+WritableUtils.getVIntSize(t1Length)+update_pos)>=bufferSize){
    	  appendFile.write(update_buf, 0, (int)update_pos);
    	  update_pos=0;
      } 
    	  System.arraycopy(temp.getData(), 0, update_buf, (int)update_pos, temp.getData().length);
    	  update_pos+=temp.getData().length;
    	  temp.reset();
      /********************************
       * same key length and same value length, need optimize later
       */
      
      //Log.info("key length : " + keyLength + " value length : " + valueLength);
//      appendFile.write(buffer.getData(), 0, buffer.getLength());       // data
    	  System.arraycopy(buffer.getData(), 0, update_buf, (int)update_pos, buffer.getData().length);
    	  update_pos+= buffer.getData().length;
      // Reset
      buffer.reset();
      
      // Update bytes written
      bytesWritten += t1Length + t2Length + t3Length + 
    		  					  WritableUtils.getVIntSize(t1Length) + 
                                  WritableUtils.getVIntSize(t2Length) + 
                                  WritableUtils.getVIntSize(t3Length);
      ++numRecordsWritten;
    }
    
    public void updateResKV (int keyhash, KEY t1, OUTVALUE t4) throws IOException {
        if (t1.getClass() != t1Class)
            throw new IOException("wrong t1 class: "+ t1.getClass()
                                  +" is not "+ t1Class);
        
      if (t4.getClass() != t4Class)
        throw new IOException("wrong t4 class: "+ t4.getClass()
                              +" is not "+ t4Class);
	  
      // Append the 'key'
      t1Serializer.serialize(t1);
      int t1Length = buffer.getLength();
      if (t1Length < 0) {
        throw new IOException("Negative key-length not allowed: " + t1Length + 
                              " for " + t1);
      }
      
      // Append the 'outvalue'
      t4Serializer.serialize(t4);
      int t4Length = buffer.getLength() - t1Length;
      if (t4Length < 0) {
        throw new IOException("Negative t4-length not allowed: " + t4Length + 
                              " for " + t4);
      }

      // Write the record out
      DataOutputBuffer temp=new DataOutputBuffer();
      WritableUtils.writeVInt(temp, t1Length);
      WritableUtils.writeVInt(temp, t4Length);
      WritableUtils.writeVInt(temp, RESULT_KV_LABEL);
//      WritableUtils.writeVInt(appendFile, t1Length);                  // key length
//      WritableUtils.writeVInt(appendFile, t4Length);                  // key length
//      WritableUtils.writeVInt(appendFile, RESULT_KV_LABEL);                // value length
      if((t1Length+t4Length+WritableUtils.getVIntSize(t4Length)
    		  +WritableUtils.getVIntSize(RESULT_KV_LABEL)+WritableUtils.getVIntSize(t1Length)+update_pos)>=bufferSize){
    	   appendFile.write(update_buf, 0, (int)update_pos);
    	  	update_pos=0;
      } 
    	  System.arraycopy(temp.getData(), 0, update_buf, (int)update_pos, temp.getLength());
    	  update_pos+=temp.getLength();
    	  temp.reset();
      /********************************
       * same key length and same value length, need optimize later
       */
      
      //Log.info("key length : " + keyLength + " value length : " + valueLength);
//      appendFile.write(buffer.getData(), 0, buffer.getLength());       // data
    	  System.arraycopy(buffer.getData(), 0, update_buf, (int)update_pos, buffer.getLength());
    	  update_pos+=buffer.getLength();
      // Reset
      buffer.reset();
      
      // Update bytes written
      bytesWritten += t1Length + t4Length +
    		  					  WritableUtils.getVIntSize(t1Length) + 
                                  WritableUtils.getVIntSize(t4Length) + 
                                  WritableUtils.getVIntSize(RESULT_KV_LABEL);
      ++numRecordsWritten;
      
      //updateShuffleKVS followed by updateResKV
	  if(!t1.equals(cachedKey)){
		  throw new IOException(t1 + "is not the key " + cachedKey + " that should be!");
	  }else{
		  indexCache.put(keyhash, new IndexEntry(cachedOffset, bytesWritten));
		  
		  //Log.info("update " + t1 + " hashkey " + keyhash + "\toffset " + cachedOffset + "\tlength " + bytesWritten);
		  
		  cachedOffset = -1;
		  bytesWritten = 0;
	  }
    }
    
    public long getLength() throws IOException { 
        return appendFile.length();
     }

      /**
       * Read upto len bytes into buf starting at offset off.
       * 
       * @param buf buffer 
       * @param off offset
       * @param len length of buffer
       * @return the no. of bytes read
       * @throws IOException
       */
      private int readIndexData(byte[] buf, int off, int len) throws IOException {
        int bytesRead = 0;
        while (bytesRead < len) {
          int n = indexIns.read(buf, off+bytesRead, len-bytesRead);
          if (n < 0) {
          	indexend = true;
            return bytesRead;
          }
          bytesRead += n;
        }
        return len;
      }
      
      private int readData(byte[] buf, int off, int len) throws IOException {
          int bytesRead = 0;
          
          while (bytesRead < len) {
        	 
            int n = dataFile.read(buf, off+bytesRead, len-bytesRead);
            if (n < 0) {
              return bytesRead;
            }
             
            bytesRead += n;
          }
          return len;
      }
      
      int readNextBlock(int minSize) throws IOException {
        int n = rejigData(databuffer, 
                    (bufferSize < minSize) ? new byte[minSize << 1] : databuffer);
        bufferSize = databuffer.length;
        return n;
      }
      
      void readIndexNextBlock(int minSize) throws IOException {
          indexbuffer = rejigIndexData(indexbuffer, 
                      (indexbufferSize < minSize) ? new byte[minSize << 1] : indexbuffer);
          indexbufferSize = indexbuffer.length;
      }
      
      private int rejigData(byte[] source, byte[] destination) 
      throws IOException{
        // Copy remaining data into the destination array
        int bytesRemaining = dataIn.getLength()-dataIn.getPosition();
        if (bytesRemaining > 0) {
          System.arraycopy(source, dataIn.getPosition(), 
              destination, 0, bytesRemaining);
        }
        
        /*
        Log.info("dataIn.getLength() " + dataIn.getLength() + " dataIn.getPosition() " + dataIn.getPosition() 
        		+ " remaining: " + bytesRemaining + " (destination.length - bytesRemaining) " + 
        		(destination.length - bytesRemaining));
        */
        
        // Read as much data as will fit from the underlying stream 
        int n = readData(destination, bytesRemaining, 
                         (destination.length - bytesRemaining));
        dataIn.reset(destination, 0, (bytesRemaining + n));
        
        
        if(this.bufstartpos==0&&bufendpos==0){
        	this.bufstartpos=this.curpos=0;
        	this.bufendpos=n;
         }else{
        	this.bufstartpos=this.bufendpos-bytesRemaining;
            this.bufendpos=this.bufendpos+n;
            this.curpos=this.bufstartpos ;
        }
       
        return n;
      }
      
      private byte[] rejigIndexData(byte[] source, byte[] destination) 
      throws IOException{
        // Copy remaining data into the destination array
        int bytesRemaining = indexIn.getLength()-indexIn.getPosition();
        if (bytesRemaining > 0) {
          System.arraycopy(source, indexIn.getPosition(), 
              destination, 0, bytesRemaining);
        }
        
        // Read as much data as will fit from the underlying stream 
        int n = readIndexData(destination, bytesRemaining, 
                         (destination.length - bytesRemaining));
        indexIn.reset(destination, 0, (bytesRemaining + n));
        
        return destination;
      }
      
      public TYPE next_old(DataInputBuffer t1, DataInputBuffer t2, DataInputBuffer t3, DataInputBuffer t4) 
      throws IOException {
        // Sanity check
        //if (eof) {
          //throw new EOFException("Completed reading " + readbytes);
        //}

        // Check if we have enough data to read lengths
        if ((dataIn.getLength() - dataIn.getPosition()) < MAX_VINT_SIZE) {
        	//Log.info("dataIn length " + dataIn.getLength() + "\tdataIn position " + dataIn.getPosition());
          int n = readNextBlock(MAX_VINT_SIZE);
         // Log.info("read next block data 1 " + n);
        }
        
        //Log.info("dataIn pos : " + dataIn.getPosition() + " length : " + dataIn.getLength() + 
        		//" file position: " + dataFile.getFilePointer() + " filelength: " + dataFile.length());
        
        // Read key and value lengths
        
        int oldPos = dataIn.getPosition();
        int t1Length = WritableUtils.readVInt(dataIn);
        int t2Length = WritableUtils.readVInt(dataIn);
        int t3Length = WritableUtils.readVInt(dataIn);
    
        int pos = dataIn.getPosition();
        readbytes += pos - oldPos;
        
        //Log.info("lengths " + t1Length + "\t" + t2Length + "\t" + t3Length);
        
        //this is the preserved output value record
        if(t3Length == RESULT_KV_LABEL){
            final int recordLength = t1Length + t2Length;
            
            //Log.info("record length " + recordLength);
            
            // Check if we have the raw key/value in the buffer
            if ((dataIn.getLength()-pos) < recordLength) {
              int n = readNextBlock(recordLength);
             // Log.info("read next block data 2 " + n);
              
              // Sanity check
              if ((dataIn.getLength() - dataIn.getPosition()) < recordLength) {
                throw new EOFException("Rec# " + recNo + ": Could read the next " +
                		                   " record");
              }
            }

            // Setup the key and value
            pos = dataIn.getPosition();
            byte[] data = dataIn.getData();
            t1.reset(data, pos, t1Length);
            t4.reset(data, (pos + t1Length), t2Length);
            
            // Position for the next record
            long skipped = dataIn.skip(recordLength);
            if (skipped != recordLength) {
              throw new IOException("Rec# " + recNo + ": Failed to skip past record " +
              		                  "of length: " + recordLength);
            }
            
            // Record the bytes read
            readbytes += recordLength;

            ++recNo;
            ++numRecordsRead;

            return TYPE.RECORDEND;
        }
        
        // Check for EOF
        if (t1Length == EOF_MARKER && t2Length == EOF_MARKER && t3Length == EOF_MARKER) {
          //eof = true;
          return TYPE.FILEEND;
        }
        
        // Sanity check
        if (t1Length < 0) {
            throw new IOException("Rec# " + recNo + ": Negative t1-length: " + 
                                  t1Length);
        }
        if (t2Length < 0) {
          throw new IOException("Rec# " + recNo + ": Negative t2-length: " + 
                                t2Length);
        }
        if (t3Length < 0) {
          throw new IOException("Rec# " + recNo + ": Negative t3-length: " + 
                                t3Length);
        }
        
        final int recordLength = t1Length + t2Length + t3Length;
        
        //Log.info("record length " + recordLength);
        
        // Check if we have the raw key/value in the buffer
        if ((dataIn.getLength()-pos) < recordLength) {
          readNextBlock(recordLength);
          
          // Sanity check
          if ((dataIn.getLength() - dataIn.getPosition()) < recordLength) {
            throw new EOFException("Rec# " + recNo + ": Could read the next " +
            		                   " record");
          }
        }

        // Setup the key and value
        pos = dataIn.getPosition();
        byte[] data = dataIn.getData();
        t1.reset(data, pos, t1Length);
        t2.reset(data, (pos + t1Length), t2Length);
        t3.reset(data, (pos + t1Length + t2Length), t3Length);
        
        // Position for the next record
        long skipped = dataIn.skip(recordLength);
        if (skipped != recordLength) {
          throw new IOException("Rec# " + recNo + ": Failed to skip past record " +
          		                  "of length: " + recordLength);
        }
        
        // Record the bytes read
        readbytes += recordLength;

        ++recNo;
        ++numRecordsRead;

        return TYPE.MORE;
      }
      public boolean seekKey_old(KEY key, int rehash, IntWritable hashcode) throws IOException{
        	//  if(rehash != 0) Log.info("rehash " + rehash);
        	  
        	  int keyhash = key.hashCode();
        	  for(int i=0; i<rehash; i++){
        		  keyhash = String.valueOf(keyhash).hashCode();
        	  }
        	  
//    	  int keyhash=((IntWritable)key).get();
          	  if(indexCache.get(keyhash) == null){
          		  //Log.info("no hashkey " + keyhash + " for key " + key + " found!");
          		  return false;
          	  }
         
          	  //Log.info("key " + key + " offset " + indexCache.get(keyhash) + " file length " + dataFile.length());
          	  dataFile.seek(indexCache.get(keyhash).offset);
          	  //Log.info("supposed offset " + indexCache.get(keyhash) + "\toffset prev " + dataFile.getFilePointer());
          	  //int n = readData(databuffer, 0, bufferSize);
          	  int entrylength = indexCache.get(keyhash).length;
          	  if(entrylength <= bufferSize - MAX_VINT_SIZE){
              	  int n = dataFile.read(databuffer, 0, entrylength + MAX_VINT_SIZE);
              	  //Log.info("read index entry length " + entrylength + " actually read " + n);
              	  dataIn.reset(databuffer, 0, n);
          	  }else{
          		 // Log.info("this is a big key, which is " + key + " length " + entrylength);
          		  
              	  int n = dataFile.read(databuffer, 0, bufferSize);
              	  //Log.info("read index entry length " + entrylength + " actually read " + n);
              	  dataIn.reset(databuffer, 0, n);
          	  }
          	  
          	  /*
          	   * for debuging
          	  String data = new String();
          	  for(int i=0; i<bufferSize; i++){
          		  data += databuffer[i] + " ";
          	  }
          	  //Log.info(data);
          	  //Log.info("offset " + dataFile.getFilePointer() + " datain size " + n + "\t" + dataIn.getLength() + "\t" + bufferSize + "\t" + dataIn.getPosition());
          	  */
          	  
          	  hashcode.set(keyhash);
          	  return true;
          }

      protected long bufstartpos;
      protected long bufendpos;
      protected long fileendpos;
      protected long curpos;
 
      public TYPE next(DataInputBuffer t1, DataInputBuffer t2, DataInputBuffer t3, DataInputBuffer t4) 
    	      throws IOException {
 
    	        if ((dataIn.getLength() - dataIn.getPosition()) < MAX_VINT_SIZE) {  
    	          int n = readNextBlock(MAX_VINT_SIZE);
    	         
    	        }
    	       
    	        int oldPos = dataIn.getPosition(); 
    	        int t1Length = WritableUtils.readVInt(dataIn);
    	        int t2Length = WritableUtils.readVInt(dataIn);
    	        int t3Length = WritableUtils.readVInt(dataIn);
    	        int pos = dataIn.getPosition();
    	         
    	        readbytes += pos - oldPos;

    	        this.curpos+=readbytes;
    	        
    	        if(t3Length == RESULT_KV_LABEL){
    	            final int recordLength = t1Length + t2Length;

    	            if ((dataIn.getLength()-pos) < recordLength) {
    	              int n = readNextBlock(recordLength);
    	              if ((dataIn.getLength() - dataIn.getPosition()) < recordLength) {
    	                throw new EOFException("Rec# " + recNo + ": Could read the next " +
    	                		                   " record");
    	              }
    	            }

    	            pos = dataIn.getPosition();
    	            byte[] data = dataIn.getData();
    	            t1.reset(data, pos, t1Length);
    	            t4.reset(data, (pos + t1Length), t2Length);
    	            
    	            this.curpos+=recordLength;
    	            
    	            long skipped = dataIn.skip(recordLength);
    	            if (skipped != recordLength) {
    	              throw new IOException("Rec# " + recNo + ": Failed to skip past record " +
    	              		                  "of length: " + recordLength);
    	            }
    	            
    	            readbytes += recordLength;

    	            ++recNo;
    	            ++numRecordsRead;

    	            return TYPE.RECORDEND;
    	        }
    	        
    	        if (t1Length == EOF_MARKER && t2Length == EOF_MARKER && t3Length == EOF_MARKER) {
    	          return TYPE.FILEEND;
    	        }
    	        if (t1Length < 0) {
    	            throw new IOException("Rec# " + recNo + ": Negative t1-length: " + 
    	                                  t1Length);
    	        }
    	        if (t2Length < 0) {
    	          throw new IOException("Rec# " + recNo + ": Negative t2-length: " + 
    	                                t2Length);
    	        }
    	        if (t3Length < 0) {
    	          throw new IOException("Rec# " + recNo + ": Negative t3-length: " + 
    	                                t3Length);
    	        }
    	        
    	        final int recordLength = t1Length + t2Length + t3Length;
    	        if ((dataIn.getLength()-pos) < recordLength) {
    	        	 
    	          readNextBlock(recordLength);
    	          if ((dataIn.getLength() - dataIn.getPosition()) < recordLength) {
    	            throw new EOFException("Rec# " + recNo + ": Could read the next " +
    	            		                   " record");
    	          }
    	        }
    	        pos = dataIn.getPosition();
    	        byte[] data = dataIn.getData();
    	        t1.reset(data, pos, t1Length);
    	        t2.reset(data, (pos + t1Length), t2Length);
    	        t3.reset(data, (pos + t1Length + t2Length), t3Length);
    	        
    	        this.curpos =this.curpos+t1Length + t2Length+t3Length;
    	        
    	        long skipped = dataIn.skip(recordLength);
    	        if (skipped != recordLength) {
    	          throw new IOException("Rec# " + recNo + ": Failed to skip past record " +
    	          		                  "of length: " + recordLength);
    	        }
    	        readbytes += recordLength;

    	        ++recNo;
    	        ++numRecordsRead;

    	        return TYPE.MORE;
    	      }
 
      public boolean seekKey(KEY key, int rehash, IntWritable hashcode) throws IOException{  

      	  int keyhash = key.hashCode();
      	  for(int i=0; i<rehash; i++){
      		  keyhash = String.valueOf(keyhash).hashCode();
      		  
      	  }
          if(indexCache.get(keyhash) == null){
        	  return false;
          }	    
          
          
          int pos=indexCache.get(keyhash).offset; 
          int entrylength = indexCache.get(keyhash).length;
          if((pos+entrylength<=this.bufendpos)&&(pos>=this.bufstartpos)){
//        	  dataIn.skip(pos-this.bufstartpos);
        	  dataIn.reset(databuffer, (int)(pos-this.bufstartpos), (int)(this.bufendpos-pos));
        	  curpos=pos;
 
          }else{
 
        	  	  dataFile.seek(pos);  
              	  int n = dataFile.read(databuffer, 0, bufferSize);
              	  dataIn.reset(databuffer, 0, n);
              	  bufstartpos=curpos=pos;
              	  bufendpos=pos+n;
  
          }
           hashcode.set(keyhash); 
 
           return true;	 
        }
 }
  
  