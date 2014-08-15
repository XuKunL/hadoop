/**
 * 
 */
package org.zkpk.hadoopday0812;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author 
 *
 */
public class PartitionByStationUsingMultipleOutputFormat extends Configured implements Tool {
	static class StationMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text>{
		private NcdcRecordParser parser=new NcdcRecordParser();
		
		
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			parser.parse(value);
			output.collect(new Text(parser.getId()), value);
		}
	}
	
	
	static class StationReducer extends MapReduceBase implements Reducer<Text,Text,NullWritable,Text>{
		
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<NullWritable, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			while(values.hasNext()){
				output.collect(NullWritable.get(), values.next());
			}
		}
		
	}
	
	static class StationNameMultipleTextOutputFormat extends MultipleTextOutputFormat<NullWritable,Text>{
		private NcdcRecordParser parser=new NcdcRecordParser();
		
		protected String generateFileNameForKeyValue(NullWritable key,Text value,String name){
			parser.parse(value);
			return parser.getId();
			
		}
		
		
	}
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int exitCode=ToolRunner.run(new PartitionByStationUsingMultipleOutputFormat(), args);
		System.exit(exitCode);

	}

	public int run(String[] args) throws Exception {
		JobConf conf=JobBuilder2.parseInputAndOutput(this, getConf(), args);
		if(conf==null){
			return -1;
		}
		// TODO Auto-generated method stub
		conf.setMapperClass(StationMapper.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setReducerClass(StationReducer.class);
		conf.setOutputKeyClass(NullWritable.class);
		conf.setOutputFormat(StationNameMultipleTextOutputFormat.class);
		JobClient.runJob(conf);
		return 0;
	}

	

}
