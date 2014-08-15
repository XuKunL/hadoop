/**
 * 
 */
package org.zkpk.hadoopday0806;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;



import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

/**
 * @author Administrator
 *
 */
public class MaxTemperature_4_4 {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		if(args.length!=2){
			System.err.println("Usage Max in out");
			System.exit(0);
		}
		JobConf conf=new JobConf(MaxTemperature_4_4.class);
		conf.setJobName("Max temperature");
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setBoolean("mapred.out.compress", true);
		conf.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);
		conf.setMapperClass(Max_mapred.class);
		conf.setCombinerClass(Max_Reducer.class);
		conf.setReducerClass(Max_Reducer.class);
		JobClient.runJob(conf);
	}

}
