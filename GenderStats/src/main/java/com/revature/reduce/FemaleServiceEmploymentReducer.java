package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FemaleServiceEmploymentReducer extends Reducer<Text, Text, Text, Text>{
	/**
	 * Finds the average change from 2000 to 2016 
	 * per year of percent of female employment in 
	 * services
	 *  
	 * Assumptions: Average change is calculated by 
	 * (mostRecentValue - earliest to 2000) / numberofYears
	 * 
	 * @param key Country
	 * @param values from 2000 - 2016 percent of female labor force in services
	 * for each country 
	 * @param context  
	 * @return
	 */

	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context)
					throws IOException, InterruptedException {
		int initialYearIndex = 0;		
		int initialYear = 2000;
		int mostRecentYear = 2016;
		for (Text value: values){

			String[] line = value.toString().split(",");
			int mostRecentYearIndex = line.length-1;

			Double mostRecentYearVal= null, initialYearVal = null;
			String strMostRecentYearVal, strInitialYearVal = "";

			Double averageChange = 0.0;
			
			do{
				if(line.length>0)strInitialYearVal = line[initialYearIndex];
				try{
					initialYearVal = Double.parseDouble(strInitialYearVal);
					if (initialYearVal <= 0) throw new NumberFormatException();
					break;
				}catch(NumberFormatException ex){
					initialYearIndex ++;
					initialYear++;
					if(initialYear> line.length) return;
				}
			}while(initialYearIndex<mostRecentYearIndex);

			do{
				strMostRecentYearVal = line[mostRecentYearIndex];
				try{
					mostRecentYearVal = Double.parseDouble(strMostRecentYearVal);
					if(mostRecentYearVal<= 0) throw new NumberFormatException();
					break;
				}catch(NumberFormatException ex){
					mostRecentYearIndex--;
					mostRecentYear--;
				}	
			}while(mostRecentYearIndex>initialYearIndex);
			
			if(mostRecentYearVal == null || initialYearVal == null || initialYear == mostRecentYear){
				return;
			}
			else{
				averageChange = (mostRecentYearVal - initialYearVal)/(mostRecentYear-initialYear);
				context.write(new Text(key), new Text(averageChange.toString()));
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