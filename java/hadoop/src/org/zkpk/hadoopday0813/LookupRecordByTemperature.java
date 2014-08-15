/**
 * 
 */
package org.zkpk.hadoopday0813;

import java.nio.file.Path;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.w3c.dom.Text;
import org.zkpk.hadoopday0812.NcdcRecordParser;

/**
 * @author Administrator
 *
 */
public class LookupRecordByTemperature extends Configured implements Tool {
	
	
	@Override
	public int run(String[] args) throws Exception {
		if(args.length!=2){
			JobBuilder3.printUsage(this, "<path> <key>");
			return -1;
		}
		Path path =new Path(args[0]);
		IntWritable key=new IntWritable(Integer.parseInt(args[1]));
		FileSystem fs =path.getFileSystem(getConf());
		
		Reader[] readers=MapFileOutputFormat.getReaders(fs, path, getConf());
		Partitioner<IntWritable,Text> partitioner =new HashPartitioner<IntWritable,Text>();
		Text val =new Text();
		Writable entry= MapFileOutputFormat.getEntry(readers, partitioner, key, val);
		if(entry==null){
			System.err.println("Key not found: " + key);
			return -1;
		}
		NcdcRecordParser parser =new NcdcRecordeParser();
		parser.parse(val.toString());
		System.out.printf("%s\t%s\n",parser.getId(),parser.getYear());
		return 0;
	}
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new LookupRecordByTemperature(), args);
		System.exit(exitCode);

	}

}
