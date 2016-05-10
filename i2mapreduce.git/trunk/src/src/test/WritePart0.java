package test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class WritePart0 {

	public static void main(String[] args) throws IOException {
		BufferedWriter write=new BufferedWriter(new FileWriter("/home/wangq/out/part0"));
		int i=0;
		String line="abcdefghijklmnopqrstuvwxyz";
		String line2="ABCDEFGHIJKLMNOPQRSTUVWXYZ";
		String line3="0123456789876543210";
		line+=line+line+line+line;
		line2+=line2+line2+line2+line2;
		line3+=line3+line3+line3+line3;
		while(i++<4_000_000){
			write.write(i+line+"\n");
			write.write(i+line2+"\n");
			write.write(i+line3+"\n");
		}
	}

}
