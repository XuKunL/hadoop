package org.zkpk.hadoopday0806;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class RandomMapper extends Mapper<Object, Text, Text, Text> {
	

	
	
	
	@Override
	protected void map(Object key, Text value,
			Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] arr=value.toString().split("\t",-1);
		if(arr.length==6){
			
			context.write(new Text(arr[1]), value);
			
		}

	}
	

}
