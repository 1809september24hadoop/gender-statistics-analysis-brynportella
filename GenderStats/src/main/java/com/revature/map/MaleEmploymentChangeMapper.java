package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaleEmploymentChangeMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
					throws IOException, InterruptedException {
		String[] line = value.toString().trim().split("\",\"");
		String indicatorCode = "SL.TLF.CACT.MA.ZS";
		String countryCode = "WLD";
		int countryCodeIndex = 1;
		int indicatorCodeIndex = 3;
		int year2000Index = 44;
		String year2000ToPresent = "";
		String outputKey = "Percent change in male labor force participation from 2000 to 2016";
		
		if (line[countryCodeIndex].equals(countryCode)){
			if(line[indicatorCodeIndex].equals(indicatorCode)){
				for (int i = year2000Index; i<line.length; i++ ){
					year2000ToPresent = (i!=line.length-1)? year2000ToPresent+cleanString(line[i])+",": year2000ToPresent+cleanString(line[i]);
				}
				context.write(new Text(outputKey), new Text(year2000ToPresent));
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
