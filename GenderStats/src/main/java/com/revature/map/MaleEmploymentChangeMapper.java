package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaleEmploymentChangeMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	public void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		String[] line = value.toString().trim().split("\",\"");
		String indicatorCode = "SL.TLF.CACT.MA.ZS";
		String countryCode = "WLD";
		int countryCodeIndex = 1;
		int indicatorCodeIndex = 3;
		int year2000Index = 44;
		int recentYearIndex = line.length-1;
		String outputKey = "Percent change in male labor force participation from 2000 to 2016";
		
		
		if (line[countryCodeIndex].equals(countryCode)){
			if(line[indicatorCodeIndex].equals(indicatorCode)){
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
