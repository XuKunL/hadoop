/**
 * 
 */
package org.zkpk.hadoopday0808;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



/**
 * @author Administrator
 *
 */
public class Url_zuoye3{
	
	/**
	 * map
	 */
	 
	static class Mapped extends Mapper<Object,Text,IntWritable,IntWritable >{
		
		IntWritable one=new IntWritable(1);
		IntWritable two=new IntWritable(2);
		IntWritable u=new IntWritable(1);
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String arr[]=value.toString().split("\t",-1);
			if (arr.length==6){
				if(arr[2].toString().matches("\\w*.\\w+(\\.\\w+)+")){
					context.write(two, u);
					if(arr[5].indexOf(arr[2])!=-1){
						context.write(one, u);
					}
				}
					
			}
		}
		
	}
	static class Reduced extends Reducer<IntWritable,IntWritable,Text,Text>{
		private Text result=new Text();
		Text chu3=new Text("URL%");
		private double chu;
		private double sum2=0;
		private double sum=0;
		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context)
				throws IOException, InterruptedException {
				for(IntWritable  value:values){
					if(key.get()==1){
						sum2+=value.get();
						
					}
					if(key.get()==2){
						sum+=value.get();
					}
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
		       chu=(sum2/sum);
			   String chu2=""+(chu*100)+"%";
			   result.set(chu2);		
		       context.write(chu3, result);
		  }
	}
	public static void main(String[] args) throws Exception{
		if(args.length!=2){
			throw new Exception ("you bu gou 2 ge");
		}
		Job job=new Job(new Configuration(),"Keyword");
		job.setJarByClass(Url_zuoye3.class);
		job.setMapperClass(Mapped.class);
		job.setReducerClass(Reduced.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		Path inputPath1=new Path(args[0]);
		Path outputPath1=new Path(args[1]);
		//MultipleInputs.addInputPath(job, inputPath1, FileinputFormat.Class, Mapped.class)
		FileInputFormat.setInputPaths(job, inputPath1);
		FileOutputFormat.setOutputPath(job, outputPath1);
		System.exit(job.waitForCompletion(true)? 0:1);
		
		
	}

}
