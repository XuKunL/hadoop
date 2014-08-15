package org.zkpk.hadoopday0811;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IdentityRed extends Reducer<IntWritable,IntWritable,Text,Text>{

	@Override
	protected void reduce(IntWritable arg0, Iterable<IntWritable> arg1,
			Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.reduce(arg0, arg1, arg2);
	}

	
}
