/**
 * 
 */
package org.zkpk.hadoopday0813;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.zkpk.hadoopday0812.NcdcRecordParser;

/**
 * @author Administrator
 *
 */
public class SortDataPreprocessor extends Configured implements Tool {
	
	static class Cleanermapper extends Mapper<LongWritable, Text,IntWritable,Text>{
		private NcdcRecordParser parser=new NcdcRecordParser();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			parser.parse(value);
			if (parser.isValidTemperature()){
				context.write(new IntWritable(parser.getAirTemperature()), value);
			}
		}
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Job job=JobBuilder3.parseInputAndOutput(this, getConf(), args);
		if(job==null){
			return -1;
		}
		job.setJarByClass(SortDataPreprocessor.class);
		job.setMapperClass(Cleanermapper.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setCompressOutput(job, true);
		//SequenceFileOutputFormat.setOutputCompressorClass(job, codecClass);
		return job.waitForCompletion(true)? 0:1;		
		
	}
	
	
	
	public static void main(String[] args) throws Exception {
		int exitCode=ToolRunner.run(new SortDataPreprocessor(), args);
		System.exit(exitCode);
	}





	

}