package org.zkpk.hadoopday0802;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class Reduc extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
	private IntWritable result =new IntWritable();
	
	@Override
	protected void reduce(IntWritable key, Iterable<IntWritable> values,Context context)
			throws IOException, InterruptedException {
		int sum=0;
		for(IntWritable count:values){
			sum+=count.get();
		}
		result.set(sum);
		context.write(key, result);
			
		
			
	}


}
