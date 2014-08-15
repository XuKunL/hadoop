package org.zkpk.hadoopday0802;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Maped extends Mapper<Object,Text,IntWritable,IntWritable>{
	IntWritable keyword=new IntWritable();
	private IntWritable one= new IntWritable(1);
	@Override
	protected void map(Object key, Text value,
			Context context)
			throws IOException, InterruptedException {
			String [] arr=value.toString().split("\t",-1);
			if(arr.length==6){
				keyword.set(arr[2].length());
				context.write(keyword,one);
			}
			
			
	}
	
}
