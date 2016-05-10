package org.apache.hadoop.mapred.io;

import java.io.IOException;
import java.io.RandomAccessFile;
 

public class Test {
	  public static void main(String[] args) throws IOException {
	        long readfilelen = 0;
	        BufferedRandomAccessFile brafReadFile, brafWriteFile;

	        brafReadFile = new BufferedRandomAccessFile("/home/wangq/part0","r",1024);
	        readfilelen = brafReadFile.getInitfilelen();
	        brafWriteFile = new BufferedRandomAccessFile("/home/wangq/part00", "rw", 1024);

	        byte buf[] = new byte[1024];
	        int readcount;

	        long start = System.currentTimeMillis();

	        while((readcount = brafReadFile.read(buf)) != -1) {
	              brafWriteFile.write(buf, 0, readcount);
	        }

	        brafWriteFile.close();
	        brafReadFile.close();

	        System.out.println("BufferedRandomAccessFile Copy & Write File: "
	                           + brafReadFile.getFilename()
	                           + "    FileSize: "
	                           + java.lang.Integer.toString((int)readfilelen >> 1024)
	                           + " (KB)    "
	                           + "Spend: "
	                           +(double)(System.currentTimeMillis()-start) / 1000
	                           + "(s)");

	        java.io.FileInputStream fdin = new java.io.FileInputStream("/home/wangq/part0");
	        java.io.BufferedInputStream bis = new java.io.BufferedInputStream(fdin, 1024);
	        java.io.DataInputStream dis = new java.io.DataInputStream(bis);

	        java.io.FileOutputStream fdout = new java.io.FileOutputStream("/home/wangq/part01");
	        java.io.BufferedOutputStream bos = new java.io.BufferedOutputStream(fdout, 1024);
	        java.io.DataOutputStream dos = new java.io.DataOutputStream(bos);

	        start = System.currentTimeMillis();

	        for (int i = 0; i < readfilelen; i++) {
	        	dos.write(dis.readByte());
	        }

	        dos.close();
	        dis.close();

	        System.out.println("DataBufferedios          Copy & Write File: "
	                           + brafReadFile.getFilename()
	                           + "    FileSize: "
	                           + java.lang.Integer.toString((int)readfilelen >> 1024)
	                           + " (KB)    "
	                           + "Spend: "
	                           + (double)(System.currentTimeMillis()-start) / 1000
	                           + "(s)");
	        
	 
	        RandomAccessFile read=new RandomAccessFile("/home/wangq/part0","rw");
	        RandomAccessFile write=new RandomAccessFile("/home/wangq/part02","rw");
	        start = System.currentTimeMillis();

	        while((readcount = read.read(buf)) != -1) {
	            write.write(buf, 0, readcount);
	        }
	        write.close();
	        read.close();
	        
	        System.out.println("RandomAccessFile         Copy & Write File: "
                    + brafReadFile.getFilename()
                    + "    FileSize: "
                    + java.lang.Integer.toString((int)readfilelen >> 1024)
                    + " (KB)    "
                    + "Spend: "
                    + (double)(System.currentTimeMillis()-start) / 1000
                    + "(s)");
	    }
}
