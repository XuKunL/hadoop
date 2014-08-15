/**
 * 
 */
package org.zkpk.hadoopday0808;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.zkpk.hadoopday0808.Sum_zuoye1.Mapped;

/**
 * @author Administrator
 *
 */
public class Rank_zuoye {

	static class Mappred extends Mapper<Object,Text,IntWritable,IntWritable>{
		IntWritable one=new IntWritable(1);
		IntWritable two=new IntWritable(2);
		IntWritable three=new IntWritable(3);
		IntWritable four=new IntWritable(4);
		IntWritable five=new IntWritable(5);
		IntWritable six=new IntWritable(6);
		IntWritable seven=new IntWritable(7);
		IntWritable eight=new IntWritable(8);
		IntWritable nine=new IntWritable(9);
		IntWritable ten=new IntWritable(10);
		IntWritable rank=new IntWritable(1);
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String arr[]=value.toString().split("\t",-1);
			if(arr.length==6){
				if(Integer.parseInt(arr[3])==1){
					context.write(one, rank);
				}
				if(Integer.parseInt(arr[3])==2){
					context.write(two, rank);
				}
				if(Integer.parseInt(arr[3])==3){
					context.write(three, rank);
				}
				if(Integer.parseInt(arr[3])==4){
					context.write(four, rank);
				}
				if(Integer.parseInt(arr[3])==5){
					context.write(five, rank);
				}
				if(Integer.parseInt(arr[3])==6){
					context.write(six, rank);
				}
				if(Integer.parseInt(arr[3])==7){
					context.write(seven, rank);
				}
				if(Integer.parseInt(arr[3])==8){
					context.write(eight, rank);
				}
				if(Integer.parseInt(arr[3])==9){
					context.write(nine, rank);
				}
				if(Integer.parseInt(arr[3])==10){
					context.write(ten, rank);
				}
			}
		}
		
	}
	static class Reduced extends Reducer<IntWritable,IntWritable,IntWritable,DoubleWritable>{
		double sum1=0;
		double sum2=0;
		double sum3=0;
		double sum4=0;
		double sum5=0;
		double sum6=0;
		double sum7=0;
		double sum8=0;
		double sum9=0;
		double sum10=0;
		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values,
				 Context context)
				throws IOException, InterruptedException {
				for(IntWritable value:values){
					if (key.get()==1){
						sum1++;
					}
					if (key.get()==2){
						sum2++;
					}
					if (key.get()==3){
						sum3++;
					}
					if (key.get()==4){
						sum4++;
					}
					if (key.get()==5){
						sum5++;
					}
					if (key.get()==6){
						sum6++;
					}
					if (key.get()==7){
						sum7++;
					}
					if (key.get()==8){
						sum8++;
					}
					if (key.get()==9){
						sum9++;
					}
					if (key.get()==10){
						sum10++;
					}
				}
		}
		@Override
		public void run(Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.run(context);
			DoubleWritable result1 =new DoubleWritable();
			DoubleWritable result2 =new DoubleWritable();
			DoubleWritable result3 =new DoubleWritable();
			DoubleWritable result4 =new DoubleWritable();
			DoubleWritable result5 =new DoubleWritable();
			DoubleWritable result6 =new DoubleWritable();
			DoubleWritable result7 =new DoubleWritable();
			DoubleWritable result8 =new DoubleWritable();
			DoubleWritable result9 =new DoubleWritable();
			DoubleWritable result10 =new DoubleWritable();
			IntWritable one=new IntWritable(1);
			IntWritable two=new IntWritable(2);
			IntWritable three=new IntWritable(3);
			IntWritable four=new IntWritable(4);
			IntWritable five=new IntWritable(5);
			IntWritable six=new IntWritable(6);
			IntWritable seven=new IntWritable(7);
			IntWritable eight=new IntWritable(8);
			IntWritable nine=new IntWritable(9);
			IntWritable ten=new IntWritable(10);
			Double sum=sum1+sum2+sum3+sum4+sum5+sum6+sum7+sum8+sum9+sum10;
			result1.set(sum1/sum);
			context.write(one, result1);
			result2.set(sum2/sum);
			context.write(two, result2);
			result3.set(sum3/sum);
			context.write(three, result3);
			result4.set(sum4/sum);
			context.write(four, result4);
			result5.set(sum5/sum);
			context.write(five, result5);
			result6.set(sum6/sum);
			context.write(six, result6);
			result7.set(sum7/sum);
			context.write(seven, result7);
			result8.set(sum8/sum);
			context.write(eight, result8);
			result9.set(sum9/sum);
			context.write(nine, result9);
			result10.set(sum10/sum);
			context.write(ten, result10);
			
		}
		
	}
	
	public static void main(String[] args) throws Exception{
		if(args.length!=2){
			throw new Exception ("you bu gou 2 ge");
		}
		Job job=new Job(new Configuration(),"Keyword");
		job.setJarByClass(Rank_zuoye.class);
		job.setMapperClass(Mappred.class);
		job.setReducerClass(Reduced.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		Path inputPath1=new Path(args[0]);
		Path outputPath1=new Path(args[1]);
		FileInputFormat.setInputPaths(job, inputPath1);
		FileOutputFormat.setOutputPath(job, outputPath1);
		System.exit(job.waitForCompletion(true)? 0:1);
	}

}
