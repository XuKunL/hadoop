/**
 * 
 */
package org.zkpk.hadoopday0814;

import org.apache.hadoop.io.Text;

/**
 * @author 
 *
 */
public class NcdcRecordParser {
	private static final int MISSING_TEMPERATURE=9999;
	public static final boolean isMissingTemperature = false;
	private String year;
	private int year1;
	private int airTemperature;
	private String quality;
	private String id;
	
	
	public void parse(String record){
		year=record.substring(15, 19);
		id=record.substring(4,15);
		year1=Integer.parseInt(year);
		String airTemperatureString;
		if(record.charAt(87)=='+'){
			airTemperatureString=record.substring(88,92);
		}else{
			airTemperatureString=record.substring(87,92);
		}
		airTemperature=Integer.parseInt(airTemperatureString);
		quality=record.substring(92,93);
	}
	public void parse(Text record){
		parse(record.toString());
	}
	
	public boolean isValidTemperature(){
		return airTemperature !=MISSING_TEMPERATURE && quality.matches("[01459]");
	}
	public boolean isMalformedTemperature(){
		return !quality.matches("[01459]");
	}
	public boolean isMissingTempreature(){
		return airTemperature == MISSING_TEMPERATURE ;
	}
	public int getAirTemperature(){
		return airTemperature;
	}
	public String getYear(){
		return year;
	}
	public String getId(){
		return id;
	}
	public String getQuality(){
		return quality;
	}
	public int getYearInt() {
		return year1;
	}
}
