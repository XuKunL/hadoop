/**
 * 
 */
package org.zkpk.hadoopday0811;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.zkpk.hadoopday0812.JobBuilder3;


/**
 * @author Administrator
 *
 */
public class Mini_new extends Configured implements Tool {

	public int run(String[] args) throws Exception{
		Job job=JobBuilder3.parseInputAndOutput(this,getConf(),args);
		if(job==null){
			return -1;
		}
		job.setJarByClass(Mini_new.class);
		job.setMapperClass(IdentityMap.class);
		job.setReducerClass(IdentityRed.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)? 0:1);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode=ToolRunner.run(new Mini_new(), args);
		System.exit(exitCode);
	}

}
