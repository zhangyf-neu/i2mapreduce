package org.apache.hadoop.mapred.lib;

import java.io.DataInput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.GlobalUniqKeyWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Projector;
import org.apache.hadoop.util.ReflectionUtils;

public class StaticDataComparator implements RawComparator {
	  private static HashMap<Class, StaticDataComparator> comparators =
			    new HashMap<Class, StaticDataComparator>(); // registry

	  private static Projector projector;
	  
	  /** Get a comparator for a {@link WritableComparable} implementation. */
	  public static synchronized StaticDataComparator get(
			  Class<? extends WritableComparable> staticclass,
			  JobConf job) {
		  projector = ReflectionUtils.newInstance(job.getProjectorClass(), job); 
		  //StaticDataComparator comparator = comparators.get(dynamicclass);
	    //if (comparator == null)
		  StaticDataComparator comparator = new StaticDataComparator(staticclass, true);
		  return comparator;
	  }

	  /** Register an optimized comparator for a {@link WritableComparable}
	   * implementation. */
	  public static synchronized void define(Class c, StaticDataComparator comparator) {
	    comparators.put(c, comparator);
	  }

	  private final Class<? extends WritableComparable> sKeyClass;
	  private final WritableComparable key1;
	  private final WritableComparable key2;
	  private WritableComparable dkey1;
	  private WritableComparable dkey2;
	  private final DataInputBuffer buffer;

	  protected StaticDataComparator(Class<? extends WritableComparable> skeyClass,
			  boolean createInstances) {
	    this.sKeyClass = skeyClass;
    	key1 = newKey();
    	key2 = newKey();
    	buffer = new DataInputBuffer();
	  }

	  /** Returns the WritableComparable implementation class. */
	  public Class<? extends WritableComparable> getKeyClass() { return sKeyClass; }

	  /** Construct a new {@link WritableComparable} instance. */
	  public WritableComparable newKey() {
	    return ReflectionUtils.newInstance(sKeyClass, null);
	  }

	  
	  /** Optimization hook.  Override this to make SequenceFile.Sorter's scream.
	   *
	   * <p>The default implementation reads the data into two {@link
	   * WritableComparable}s (using {@link
	   * Writable#readFields(DataInput)}, then calls {@link
	   * #compare(WritableComparable,WritableComparable)}.
	   */
	  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	    try {
	      buffer.reset(b1, s1, l1);                   // parse key1
	      key1.readFields(buffer);
	      
	      buffer.reset(b2, s2, l2);                   // parse key2
	      key2.readFields(buffer);
	      
	    } catch (IOException e) {
	      throw new RuntimeException(e);
	    }
	    
	    return compare(key1, key2);                   // compare them
	  }

	  /** Compare two WritableComparables.
	   *
	   * <p> The default implementation uses the natural ordering, calling {@link
	   * Comparable#compareTo(Object)}. */
	  @SuppressWarnings("unchecked")
	  public int compare(WritableComparable a, WritableComparable b) {
		  dkey1 = projector.project(a);
		  dkey2 = projector.project(b);
		  
		  if(dkey1 instanceof GlobalUniqKeyWritable){
			  return a.compareTo(b);
		  }else{
			  int res = dkey1.compareTo(dkey2);
			  if(res == 0){
				  return a.compareTo(b);
			  }else{
				  return res;
			  }
		  }
	    
	  }

	  public int compare(Object a, Object b) {
	    return compare((WritableComparable)a, (WritableComparable)b);
	  }

	  /** Lexicographic order of binary data. */
	  public static int compareBytes(byte[] b1, int s1, int l1,
	                                 byte[] b2, int s2, int l2) {
	    int end1 = s1 + l1;
	    int end2 = s2 + l2;
	    for (int i = s1, j = s2; i < end1 && j < end2; i++, j++) {
	      int a = (b1[i] & 0xff);
	      int b = (b2[j] & 0xff);
	      if (a != b) {
	        return a - b;
	      }
	    }
	    return l1 - l2;
	  }
	  
	  /** Compute hash for binary data. */
	  public static int hashBytes(byte[] bytes, int offset, int length) {
	    int hash = 1;
	    for (int i = offset; i < offset + length; i++)
	      hash = (31 * hash) + (int)bytes[i];
	    return hash;
	  }
	  
	  /** Compute hash for binary data. */
	  public static int hashBytes(byte[] bytes, int length) {
	    return hashBytes(bytes, 0, length);
	  }

	  /** Parse an unsigned short from a byte array. */
	  public static int readUnsignedShort(byte[] bytes, int start) {
	    return (((bytes[start]   & 0xff) <<  8) +
	            ((bytes[start+1] & 0xff)));
	  }

	  /** Parse an integer from a byte array. */
	  public static int readInt(byte[] bytes, int start) {
	    return (((bytes[start  ] & 0xff) << 24) +
	            ((bytes[start+1] & 0xff) << 16) +
	            ((bytes[start+2] & 0xff) <<  8) +
	            ((bytes[start+3] & 0xff)));

	  }

	  /** Parse a float from a byte array. */
	  public static float readFloat(byte[] bytes, int start) {
	    return Float.intBitsToFloat(readInt(bytes, start));
	  }

	  /** Parse a long from a byte array. */
	  public static long readLong(byte[] bytes, int start) {
	    return ((long)(readInt(bytes, start)) << 32) +
	      (readInt(bytes, start+4) & 0xFFFFFFFFL);
	  }

	  /** Parse a double from a byte array. */
	  public static double readDouble(byte[] bytes, int start) {
	    return Double.longBitsToDouble(readLong(bytes, start));
	  }

	  /**
	   * Reads a zero-compressed encoded long from a byte array and returns it.
	   * @param bytes byte array with decode long
	   * @param start starting index
	   * @throws java.io.IOException 
	   * @return deserialized long
	   */
	  public static long readVLong(byte[] bytes, int start) throws IOException {
	    int len = bytes[start];
	    if (len >= -112) {
	      return len;
	    }
	    boolean isNegative = (len < -120);
	    len = isNegative ? -(len + 120) : -(len + 112);
	    if (start+1+len>bytes.length)
	      throw new IOException(
	                            "Not enough number of bytes for a zero-compressed integer");
	    long i = 0;
	    for (int idx = 0; idx < len; idx++) {
	      i = i << 8;
	      i = i | (bytes[start+1+idx] & 0xFF);
	    }
	    return (isNegative ? (i ^ -1L) : i);
	  }
	  
	  /**
	   * Reads a zero-compressed encoded integer from a byte array and returns it.
	   * @param bytes byte array with the encoded integer
	   * @param start start index
	   * @throws java.io.IOException 
	   * @return deserialized integer
	   */
	  public static int readVInt(byte[] bytes, int start) throws IOException {
	    return (int) readVLong(bytes, start);
	  }
}
