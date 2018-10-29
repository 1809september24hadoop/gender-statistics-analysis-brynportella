package com.revature.map;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class FemaleGraduateMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	/** 
	 * If a record has cumulative female educational info
	 * about graduating from upper secondary where the most recent percentage is less 
	 * than  30% map this value 
	 * to the key of the year and country. 
	 * 
	 * Assumptions: Countries without data shouldn't be assumed to be less than 30% 
	 * and are there for mapped with a value of "no data available"
	 * The most recent data available is the most relevant to consider.
	 * The year of this data should be taken into account- so it is output as part of 
	 * the key. 
	 *
	 * 
	 * @param key implicitly defined longwirtable
	 * @param value file line passed in
	 * @param context reference to intermediate output
	 * 
	 * 
	 * @return 
	 */
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] line = value.toString().trim().split("\",\"");
		String indicatorCode = "SE.SEC.CUAT.UP.FE.ZS";
		Long countryInfoMinIndex = 10983756L;
		int graduationRate = 30;
		int year = 1960;
		int outputYear = year;
		String outputKey = line[0].substring(1);
		Deque<String> gradRateStack = new ArrayDeque();
		for (int i =0; i<line.length; i++) {
			String element = line[i];			
			if(element.contains(indicatorCode)) {
				for (int j = i+1 ; j<line.length; j++) {
					String percentage = cleanString(line[j].trim()); 
					if(percentage.length()>1){
						gradRateStack.push(percentage);	
						outputYear = year;
					}
					year++;
				}
				if(gradRateStack.isEmpty() ){
					if (key.get()>countryInfoMinIndex) context.write(new Text(outputKey), new Text("No data available"));
					return;
				}
				String mostRecentPercentage = gradRateStack.pop();
				try{
					Double doublePercentage = Double.parseDouble(mostRecentPercentage);
					if(doublePercentage<graduationRate){
						context.write(new Text(outputKey+" "+outputYear), new Text(doublePercentage.toString()));
					}
				}catch(NumberFormatException ex){
					return;
				}	
			}else if(i>10){
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