/**
 * 
 */
package org.zkpk.hadoopday0807;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

/**
 * @author Administrator
 *
 */
public class SequenceFile_4_12 {
	private static final String[] DATA =
		{ 
		"57375476989eea12893c0c3811607bcf",
		"66c5bb7774e31d0a22278249b26bc83a",
		"b97920521c78de70ac38e3713f524b50",
		"6961d0c97fe93701fc9c0d861d096cd9",
		"f2f5a21c764aebde1e8afcc2871e086f",
		"96994a0480e7e1edcaef67b20d8816b7",
		"698956eb07815439fe5f46e9a4503997",
		"599cd26984f72ee68b2b6ebefccf6aed",
		"f577230df7b6c532837cd16ab731f874",
		"285f88780dd0659f5fc8acc7cc4949f2",
		"f4ba3f337efb1cc469fcd0b34feff9fb",
		"3d1acc7235374d531de1ca885df5e711",
		"dbce4101683913365648eba6a85b6273",
		"58e7d0caec23bcb4daa7bbcc4d37f008",
		"a3b83dc38b2bbc35660dffcab4ed9da8",
		"b89952902d7821db37e8999776b32427",
		"7c54c43f3a8a0af0951c26d94a57d6c8",
		"2d6c22c084a501c0b8f7f0a845aefd9f",
		"11097724dae8b9fdcc60bd6fa4ce4df2",
		"1d374b57fbbc81aa0cc38e6f4efb88ec",
		"76029a8965e815b413cba0b50d2ec2b0",
		"22201bdc15845bfb33384efc3a283ef4",
		"e0d255845fc9e66b2a25c43a70de4a9a",
		"b89952902d7821db37e8999776b32427",
		"072fa3643c91b29bd586aff29b402161",
		"f31f594bd1f3147298bd952ba35de84d",
		"26d2e31d48f527e34c082bc0de591e0e",
		"66c5bb7774e31d0a22278249b26bc83a",
		"f1b7efe9428b9074f79d3e91ecc6385e",
		"16c3b69cc93e838f89895b49643cef1d",
	};
	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException {
		String uri=args[0];
		Configuration conf=new Configuration();
		FileSystem fs =FileSystem.get(URI.create(uri),conf);
		Path path=new Path(uri);
		IntWritable key=new IntWritable();
		Text value=new Text();
		SequenceFile.Writer writer=null;
		try{
			writer=SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
			for (int i=0;i<100;i++){
				key.set(100-i);
				value.set(DATA[i % DATA.length]);
				System.out.printf("[%s]\t%s\t%s\n",writer.getLength(),key,value);
				writer.append(key, value);
			}
		}finally{
			IOUtils.closeStream(writer);
		}
	}

}
