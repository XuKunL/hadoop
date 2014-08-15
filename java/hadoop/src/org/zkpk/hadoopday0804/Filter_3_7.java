/**
 * 
 */
package org.zkpk.hadoopday0804;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;




/**
 * @author Administrator
 *
 */
public class Filter_3_7 implements PathFilter{

	private final String regex;
	public  Filter_3_7(String regex){
		this.regex=regex;
	}
	public boolean accept (Path path){
		return !path.toString().matches(regex);
	}
	public static void main (String args[]) throws Exception{
		Filter_3_7 f =new Filter_3_7(args[0]);
		Path p=new Path(args[1]);
		if(f.accept(p)){
			System.out.println("no");
		}else{
			System.out.println("yes");
		}
	}

}
