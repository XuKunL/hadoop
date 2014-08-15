/**
 * 
 */
package org.zkpk.hadoopday0811;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

/**
 * @author Administrator
 *
 */
public class JobBuilder3 {

	static Job parseInputAndOutput(Tool tool,Configuration job,String[] args) throws IOException{
		if(args.length!=2){
			printUsage(tool,"<input> <output>");
			return null;
		}
		Job job1 =new Job(job);
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		return job1;
	}
	static void printUsage(Tool tool,String extraArgsUsage){
		System.err.printf("Usage: %s[genericOptions] %s\n\n",tool.getClass().getSimpleName(),extraArgsUsage);
		GenericOptionsParser.printGenericCommandUsage(System.err);
		
	}

}
