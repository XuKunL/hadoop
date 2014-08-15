/**
 * 
 */
package org.zkpk.hadoopday0805;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;



/**
 * @author Administrator
 *
 */
public class Coherency {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String uri=args[0];
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(URI.create(uri),conf);
		Path p=new Path(uri);
		FSDataOutputStream out=fs.create(p);
		out.write("content".getBytes("UTF-8"));
		out.flush();
		out.sync();
		out.close();
		long a=fs.getFileStatus(p).getLen();
		long b="content".length();
		if (a==b){
			System.out.println("true");
		}else{
			System.out.println("false");
		}
	}

}
