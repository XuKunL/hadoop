/**
 * 
 */
package org.zkpk.hadoopday0814;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Administrator
 *
 */
public class SortByTemperatureUsingTotalOrderPartitioner extends Configured implements Tool {
	
	
	
	@Override
	public int run(String[] args) throws Exception {
		JobConf conf =JobBuilder3.parseInputAndOutput(this, getConf(), args);
		if(conf==null){
			return -1;
		}
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		
		conf.setPartitionerClass(TotalOrderPartitioner.class);
		
		InputSampler.Sampler<IntWritable,Text> sampler=new InputSampler.RandomSampler<IntWritable,Text>(0.1, 1000,10);
		
		Path input =FileInputFormat.getInputPaths(conf)[0];
		input = input.makeQualified(input.getFileSystem(conf));
		
		Path partitionFile =new Path(input,"_partitions");
		
		TotalOrderPartitioner.setPartitionFile(conf, partitionFile);
		InputSampler.writePartitionFile(conf, sampler);
		
		URI partitionUri =new URI(partitionFile.toString()+ "#_partitions");
		DistributedCache.addCacheFile(partitionUri, conf);
		DistributedCache.createSymlink(conf);
		
		JobClient.runJob(conf);
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode=ToolRunner.run(new SortByTemperatureUsingTotalOrderPartitioner(), args);
		System.exit(exitCode);

	}

}
