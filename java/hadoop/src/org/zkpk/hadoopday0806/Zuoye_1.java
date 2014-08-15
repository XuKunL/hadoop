/**
 * 
 */
package org.zkpk.hadoopday0806;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Zuoye_1 {
	/**
	 * job map1
	 *
	 */
	static class Mappered extends Mapper<Object,Text,Text,IntWritable>{
		private Text uid=new Text();
		private IntWritable one=new IntWritable(1);
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String arr[]=value.toString().split("\t",-1);
			if(arr.length==6){
				uid.set(arr[1]);
			}
			context.write(uid, one);
		}
		
	}
	/**
	 * job reduce1
	 *
	 */
	static class Reduced extends Reducer<Text,IntWritable,Text,IntWritable>{
		private IntWritable result=new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum=0;
			int sum2=0;
			for(IntWritable count:values){
				sum+=count.get();
				sum2++;
			}
		 	if(sum>2){
			result.set(sum2);
			context.write(key, result);
			}
		}
		
	}
	/**
	 * job2 map2
	 *
	 */
	static class Mappered2 extends Mapper <Object,Text,Text,IntWritable>{
		private Text sumid=new Text("Sumid");
		private IntWritable one=new IntWritable(1);

		@Override
		protected void map(Object key, Text value,
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String arr[]=value.toString().split("\t",-1);
			if(arr.length==2)	
			context.write(sumid, one);
		}
		
	}
	/**
	 * job2 reduce2
	 *
	 */
	static class Reduced2 extends Reducer<Text,IntWritable,Text,IntWritable>{
		private IntWritable result=new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum=0;
			for(IntWritable count:values){
				sum+=count.get();
			}
			result.set(sum);
			context.write(key, result);
		}
		
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		if(args.length!=3){
			throw new Exception ("you bu gou 3 ge");
		}
		/**
		 * job1
		 */
		Job job=new Job(new Configuration(),"Keyword");
		job.setJarByClass(Zuoye_1.class);
		job.setMapperClass(Mappered.class);
		job.setReducerClass(Reduced.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		Path inputPath1=new Path(args[0]);
		Path outputPath1=new Path(args[1]);
		FileInputFormat.setInputPaths(job, inputPath1);
		FileOutputFormat.setOutputPath(job, outputPath1);
		job.waitForCompletion(true);
		/**
		 * job2
		 */
		Job job2=new Job(new Configuration(),"Keyword");
		job2.setJarByClass(Zuoye_1.class);
		job2.setMapperClass(Mappered2.class);
		job2.setReducerClass(Reduced2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job2, outputPath1);
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		System.exit(job2.waitForCompletion(true)? 0:1);
		
	}

}
