/**
 * 
 */
package org.zkpk.hadoopday0804;


import java.io.InputStream;
import java.net.URL;

 
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

/**
 * @author Administrator
 *
 */
public class UrlCat {
	static{
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		
	}
	 public static void main (String [] args) throws Exception {
		 InputStream in = null;
		 try{
			 in=new URL(args[0]).openStream();
			 IOUtils.copyBytes(in,System.out,4096,false);
			 
		 }finally{
			 org.apache.hadoop.io.IOUtils.closeStream(in);
		 }
	 }
}
