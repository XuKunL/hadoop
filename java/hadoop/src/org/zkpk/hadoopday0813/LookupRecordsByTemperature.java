package org.zkpk.hadoopday0813;

import java.io.Reader;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.zkpk.hadoopday0812.NcdcRecordParser;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils.Text;

public class LookupRecordsByTemperature extends Configured implements Tool{

	
	@Override
	public int run(String[] args) throws Exception {
		if(args.length!=2){
			JobBuilder3.printUsage(this, "<path> <key>");
			return -1;
		}
		Path path = new Path(args[0]);
		IntWritable key =new IntWritable(Integer.parseInt(args[1]));
		FileSystem fs = path.getFileSystem(getConf());
		
		Reader[] readers = MapFileOutputFPartitioner<K2, V2>aders(fs, path, getConf());
		Partitioner<IntWritable,Text> partitioner=new HashPartitioner<IntWritable,Text>();
		Text val=new Text();
		
		Reader reader=readers[partitioner.getPartition(key, val, readers.length)];
		Writable entry =reader.get(key,val);
		if(entry==null){
			System.err.println("key not found: "+key);
			return -1;
		}
		NcdcRecordParser parser=new NcdcRecordParser();
		IntWritable nextkey=new IntWritable();
		do{
			parser.parse(val.toString());
			System.out.printf("%s\t%s\n", parser.getId(),parser.getYear());
		}while(reader.next(nextkey,val) && key.equals(nextkey));
		return 0;
		
	}

	public static void main(String[] args) throws Exception {
		int exitCode=ToolRunner.run(new LookupRecordsByTemperature(), args);
		System.exit(exitCode);

	}

}
