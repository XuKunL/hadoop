/**
 * 
 */
package org.zkpk.hadoopday0806;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

/**
 * @author Administrator
 *
 */
public class MaxTemperature {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		if(args.length !=2){
			System.err.println("Usage: in out");
			System.exit(-1);
		}
		JobConf conf=new JobConf (MaxTemperature.class);
		conf.setJobName("Max temperature");
		
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		conf.setMapperClass(Max_mapred.class);
		conf.setReducerClass(Max_Reducer.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		JobClient.runJob(conf);
	}

}
