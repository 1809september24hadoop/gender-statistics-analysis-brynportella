package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaleEmploymentChangeReducer extends Reducer<Text, Text, Text, DoubleWritable>{
	/**
	 * Finds the percent change from 2000 to 2016 in percent of male labor force participation
	 *  
	 * Assumptions: Percent change is calculated by 
	 * (value in 2016 - value in 2000) / value in 2000
	 * 
	 * @param key Percent change in male labor force participation
	 * @param values List of records which include percent of female labor force participation 
	 * for each year 
	 * @param context  
	 * @return
	 */
	
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		int year2000Index = 0;
		String outputKey = "Percent change in male labor force participation from 2000 to 2016";
		for (Text value: values){
			String[] line = value.toString().split(",");
			int recentYearIndex = line.length-1;
			try{
				
				
				String recentYearStr = cleanString(line[recentYearIndex].trim());
				Double recentYearVal = Double.parseDouble(recentYearStr);
				
				String year2000Str = cleanString(line[year2000Index]);
				Double year2000Val = Double.parseDouble(year2000Str);
				
				Double percentChange = 100*((recentYearVal-year2000Val)/(year2000Val));
				
				context.write(new Text(outputKey), new DoubleWritable(percentChange));
				
			}catch(NumberFormatException ex){
				return;
			}
			
		}
	}
	
	private String cleanString(String word){
		String newWord = "";
		for (char c: word.toCharArray()){
			if(Character.isDigit(c)|| c=='.'){
				newWord += c;
			}
		}
		return newWord;
	}
}
