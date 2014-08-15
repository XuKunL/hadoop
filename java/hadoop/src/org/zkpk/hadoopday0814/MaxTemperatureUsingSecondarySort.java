package org.zkpk.hadoopday0814;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class MaxTemperatureUsingSecondarySort extends Configured implements Tool{
	
	static class MaxTemperatureMapper extends Mapper<LongWritable,Text,IntPair,NullWritable>{
		private NcdcRecordParser parser=new NcdcRecordParser();
		
		public void map(LongWritable key,Text value,Context context)throws IOException, InterruptedException{
			parser.parse(value);
			if(parser.isValidTemperature()){
				context.write(new IntPair(parser.getYearInt(),+parser.getAirTemperature()),NullWritable.get());
			}
		}
	}
	static class MaxTemperatureReducer extends Reducer<IntPair,NullWritable,IntPair,NullWritable>{
		public void reduce(IntPair key,Iterator<NullWritable> values,Context context)throws IOException, InterruptedException{
			context.write(key,NullWritable.get());
		}
	}
	
	public static class FirstPartitioner extends Partitioner<IntPair,NullWritable>{
		
		//public void configure(Job job){}
		@Override
		public int getPartition(IntPair key,NullWritable value,int numPartitions) {
			return Math.abs(key.getFirst().get() * 127) % numPartitions;
		}

	}
	
	public static class KeyComparator extends WritableComparator{
		protected KeyComparator(){
			super(IntPair.class,true);
		}
		@Override
		public int compare(WritableComparable w1,WritableComparable w2){
			IntPair ip1 = (IntPair) w1;
			IntPair ip2 = (IntPair) w2;
			int cmp=IntPair.compare(ip1.getFirst(),ip2.getFirst());
			if(cmp !=0){
				return cmp;
			}
			return -IntPair.compare(ip1.getSecond(),ip2.getSecond());
		}
	}
	public static class GroupComparator extends WritableComparator{
		protected GroupComparator(){
			super (IntPair.class,true);
		}
		@Override
		public int compare(WritableComparable w1,WritableComparable w2){
			IntPair ip1 = (IntPair) w1;
			IntPair ip2 = (IntPair) w2;
			return IntPair.compare(ip1.getFirst(),ip2.getFirst());
		}
	}
		@Override
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
			Job conf= JobBuilder3.parseInputAndOutput(this, getConf(), args);
			if(conf==null){
				return -1;
			}
			conf.setJarByClass(MaxTemperatureUsingSecondarySort.class);
			conf.setMapperClass(MaxTemperatureMapper.class);
			conf.setPartitionerClass(FirstPartitioner.class);
			conf.setSortComparatorClass(KeyComparator.class);
			conf.setGroupingComparatorClass(GroupComparator.class);
			
			conf.setReducerClass(MaxTemperatureReducer.class);
			conf.setOutputKeyClass(IntPair.class);
			conf.setOutputValueClass(NullWritable.class);
			
			return conf.waitForCompletion(true)? 0:1;
			
			
			
			
		}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MaxTemperatureUsingSecondarySort(), args);
		System.exit(exitCode);

	}

}
