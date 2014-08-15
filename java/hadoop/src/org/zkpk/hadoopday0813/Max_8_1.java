/**
 * 
 */
package org.zkpk.hadoopday0813;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.zkpk.hadoopday0812.NcdcRecordParser;



/**
 * @author Administrator
 *
 */
public class Max_8_1 extends Configured implements Tool{
	
	enum Temperature{
		MISSING,
		MALFORMED
	}
	static class MaxTemperatureMapperWithCounters extends Mapper<LongWritable,Text,Text,IntWritable>{
		private NcdcRecordParser parser =new NcdcRecordParser();
		private static Counter ff=null;
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			parser.parse(value);
			if(parser.isValidTemperature()){
				int airTemperature=parser.getAirTemperature();
				context.write(new Text(parser.getYear()), new IntWritable(airTemperature));
			}else if (parser.isMalformedTemperature()){
				System.err.println("Ignoring possibly corrupt input:"+ value);
				context.getCounter(Temperature.MALFORMED);
				ff.increment(1);
			}else if(parser.isMissingTemperature){
				context.getCounter(Temperature.MISSING);
				ff.increment(1);
			}
			context.getCounter("TemperatureQulity", parser.getQuality());
		}
		
	}
	public int run(String []args)throws IOException, ClassNotFoundException, InterruptedException {
		Job conf=JobBuilder3.parseInputAndOutput(this, getConf(), args);
		if(conf == null){
			return -1;
		}
		conf.setJarByClass(MaxTemperatureMapperWithCounters.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setMapperClass(MaxTemperatureMapperWithCounters.class);
		conf.setReducerClass(Max_Reducer.class);
		return conf.waitForCompletion(true)? 0:1;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		int exitCode=ToolRunner.run(new Max_8_1(), args);
		System.exit(exitCode);
	}

}
