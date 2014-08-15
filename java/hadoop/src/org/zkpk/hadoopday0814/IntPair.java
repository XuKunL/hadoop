package org.zkpk.hadoopday0814;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class IntPair implements WritableComparable<IntPair>{
	private IntWritable first;
	private IntWritable second;
	
	public IntPair(){
		set(new IntWritable(),new IntWritable());
	}
	
	public IntPair(String first,String second){
		set(new IntWritable(),new IntWritable());
	}
	
	public IntPair(IntWritable first,IntWritable second){
		set(first,second);
	}
	
	public IntPair(int first, int second) {
		IntWritable i1=new IntWritable(first);
		IntWritable i2=new IntWritable(second);
		set(i1,i2);
	}

	public void set(IntWritable first,IntWritable second){
		this.first=first;
		this.second=second;
	}
	
	public IntWritable getFirst(){
		return first;
	}
	
	public IntWritable getSecond(){
		return second;
	}
//	public int getFirst2(){
//		return first.get();
//	}
//	public int getSecond2(){
//		return second.get();
//	}
	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}

	@Override
	public int compareTo(IntPair tp) {
		int cmp = first.compareTo(tp.first);
		if(cmp!=0){
			return cmp;
		}
		return second.compareTo(tp.second);
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof IntPair){
			IntPair tp=(IntPair) obj;
			return first.equals(tp.first) && second.equals(tp.second);
		}
		return false;
	}

	@Override
	public String toString() {
		return first + "\t" + second;
	}

	@Override
	public int hashCode() {
		return first.hashCode() * 163 + second.hashCode();
	}

	public static int compare(IntWritable first1, IntWritable first2) {
		
		return first1.compareTo(first2);
	}	
	
	
}

