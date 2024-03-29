/**
 * 
 */
package org.zkpk.hadoopday0808;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * @author Administrator
 *
 */
public class Xian_zuoye2 {
	static class Mapped extends Mapper<Object,Text,Text,Text>{
		Text uid=new Text();
		Text rec=new Text(); 
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String arr[]=value.toString().split("\t",-1);
			if(arr.length==6){
				String keyw="�ɽ�������";
				if(arr[2].indexOf(keyw)!=-1){
					uid.set(arr[1]);
					rec.set(value.toString());
					context.write(uid, rec);
				}
			}
		}
		
	}
	static class Reduced extends Reducer<Text,Text,IntWritable,Text>{
		private List<String> list1=new ArrayList<String>();
		private List<String> list2=new ArrayList<String>();
		private IntWritable one=new IntWritable(1);
		private IntWritable two=new IntWritable(2);
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context context)
				throws IOException, InterruptedException {
				List<String> record=new ArrayList<String>();
				for(Text text:values){
					record.add(text.toString());
				}
				if(record.size()>3){
					if(record.size()>list2.size()){
						if(record.size()>list1.size()){
							list2.clear();
							list2.addAll(list1);
							list1.clear();
							list1.addAll(record);
						}else{
							list2.clear();
							list2.addAll(record);
						}
					}
				}
		}
		@Override
		public void run(Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.run(context);
			for(String s1:list1){
				context.write(one, new Text(s1));
			}
			for(String s2:list2){
				context.write(two, new Text(s2));
			}
		}
		
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if(args.length!=2){
			throw new Exception ("you bu gou 2 ge");
		}
		Job job=new Job(new Configuration(),"Keyword");
		job.setJarByClass(Xian_zuoye2.class);
		job.setMapperClass(Mapped.class);
		job.setReducerClass(Reduced.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		Path inputPath1=new Path(args[0]);
		Path outputPath1=new Path(args[1]);
		FileInputFormat.setInputPaths(job, inputPath1);
		FileOutputFormat.setOutputPath(job, outputPath1);
		System.exit(job.waitForCompletion(true)? 0:1);
	}

}
