package org.zkpk.hadoopday0812;

/**
 * 
 */

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author xkl
 *
 */
public class PartitionByStationUsingMultipleOutputs extends Configured implements Tool {
	static class StationMapper extends Mapper<LongWritable,Text,Text,Text>{
		private NcdcRecordParser parser=new NcdcRecordParser();

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			parser.parse(value);
			context.write(new Text(parser.getId()), value);
		}

	}
	static class MultipleOutputsReducer extends Reducer<Text,Text,NullWritable,Text>{
		private MultipleOutputs multipleOutputs;
		
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			multipleOutputs=new MultipleOutputs(context);
		}
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			for(Text value:values){
				multipleOutputs.write("station", key, value,"station_"+key);
			//	context.write(NullWritable.get(), value);
			}
		}

		

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			multipleOutputs.close();
		}
		
		
	}
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int exitCode =ToolRunner.run(new PartitionByStationUsingMultipleOutputs(), args);
		System.exit(exitCode);

	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job conf=JobBuilder3.parseInputAndOutput(this, getConf(), args);
		if(conf==null){
			return -1;
		}
		
		conf.setJarByClass(PartitionByStationUsingMultipleOutputs.class);
		conf.setMapperClass(StationMapper.class);
		conf.setReducerClass(MultipleOutputsReducer.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		MultipleOutputs.addNamedOutput(conf, "station", TextOutputFormat.class, NullWritable.class, Text.class);
		
		return (conf.waitForCompletion(true)?0:1);
	}

}

