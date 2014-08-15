/**
 * 
 */
package org.zkpk.hadoopday0813;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;


/**
 * @author Administrator
 *
 */
public class SortByTemperatureToMapFile extends Configured implements Tool{
	
	
	@Override
	public int run(String[] args) throws Exception {
		JobConf conf=JobBuilder3.parseInputAndOutput(this, getConf(), args);
		if(conf==null){
			return -1;
		}
		conf.setInputFormatClass(SequenceFileInputFormat.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputFormatClass(MapFileOutputFormat.class);
		return 0;
		
	}

	public static void main(String[] args) throws Exception {
		int exitCode=ToolRunner.run(new SortByTemperatureToMapFile(), args);
		System.exit(exitCode);

	}

}
