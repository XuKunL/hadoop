/**
 * 
 */
package org.zkpk.hadoopday0806;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * @author Administrator
 *
 */
public class Compressor_4_1 {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		String codeClassname = args[0];
		Class<?> codecClass = Class.forName(codeClassname);
		Configuration conf=new Configuration();
		CompressionCodec codec =(CompressionCodec)
				ReflectionUtils.newInstance(codecClass, conf);
				CompressionOutputStream out= codec.createOutputStream(System.out);
				IOUtils.copyBytes(System.in,out,4096,false);
				out.finish();
	}

}
