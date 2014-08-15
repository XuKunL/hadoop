/**
 * 
 */
package org.zkpk.hadoopday0806;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Zuoye_2 {
	/**
	 * job map1
	 *
	 */
	static int sumsum=0;
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
	static class Reduced extends Reducer<Text,IntWritable,Text,DoubleWritable>{
	   DoubleWritable result=new DoubleWritable();
		
		double sum2=0;
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			double sum=0;
			sumsum++;
			for(IntWritable count:values){
				sum+=count.get();
			}
			if(sum>2){
				sum2++;
			}
			
		}
		public void run(Context context) throws IOException, InterruptedException {
		    setup(context);
		    try {
		      while (context.nextKey()) {
		        reduce(context.getCurrentKey(), context.getValues(), context);
		      }
		    } finally {
		      cleanup(context);
		    }
		    double chu=(sum2/sumsum);
		    System.out.println(sumsum);
		    String chu2=""+(chu*100)+"%";
		    Text chu3=new Text();
		    chu3.set(chu2.toString());
		    result.set(sum2);
		    context.write(chu3, result);
		  }
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		if(args.length!=2){
			throw new Exception ("you bu gou 2 ge");
		}
		/**
		 * job1
		 */
		Job job=new Job(new Configuration(),"Keyword");
		job.setJarByClass(Zuoye_2.class);
		job.setMapperClass(Mappered.class);
		job.setReducerClass(Reduced.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		Path inputPath1=new Path(args[0]);
		Path outputPath1=new Path(args[1]);
		FileInputFormat.setInputPaths(job, inputPath1);
		FileOutputFormat.setOutputPath(job, outputPath1);
		System.exit(job.waitForCompletion(true)? 0:1);
		
	}

}