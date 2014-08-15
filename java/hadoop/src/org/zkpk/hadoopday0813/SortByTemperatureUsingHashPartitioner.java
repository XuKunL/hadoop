/**
 * 
 */
package org.zkpk.hadoopday0813;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Administrator
 *
 */
public class SortByTemperatureUsingHashPartitioner extends Configured implements Tool{

	
	@Override
	public int run(String[] args) throws Exception {
		Job job=JobBuilder3.parseInputAndOutput(this, getConf(), args);
		if(job==null){
			return -1;
		}
		job.setJarByClass(SortByTemperatureUsingHashPartitioner.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setCompressOutput(job, true);
		return job.waitForCompletion(true)? 0:1;
	}
	
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SortByTemperatureUsingHashPartitioner(), args);
		System.exit(exitCode);
	}

}
