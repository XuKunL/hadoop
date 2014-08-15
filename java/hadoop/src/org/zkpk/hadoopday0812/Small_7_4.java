package org.zkpk.hadoopday0812;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;





public class Small_7_4 extends Configured implements Tool{

	static class SequenceFileMapper extends Mapper<NullWritable,BytesWritable,Text,BytesWritable>{
		private Job job;
		private Context ccontext;
		
		public void configure (Job job){
			this.job=job;
		}
		@Override
		protected void map(NullWritable key, BytesWritable value,
				Context context) throws IOException, InterruptedException {
			FileSplit fileSplit =(FileSplit) ccontext.getInputSplit();
			String filename=fileSplit.getPath().toString();
			context.write(new Text(filename),value);
		}
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stu
			this.ccontext=context;
		}


		
	}

	//@Override
	public int run(String []args) throws IOException, InterruptedException, ClassNotFoundException {
		Job job=JobBuilder3.parseInputAndOutput(this, getConf(), args);
		if(job == null){
			return -1;
		}
		job.setJarByClass(Small_7_4.class);
		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapperClass(SequenceFileMapper.class);
		job.setReducerClass(IdentityReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		
		return job.waitForCompletion(true)? 0:1;
	  }
	
	
	
	public static void main (String args[]) throws Exception{
		int exitCode =ToolRunner.run(new Small_7_4(), args);
		System.exit(exitCode);
	}
}
