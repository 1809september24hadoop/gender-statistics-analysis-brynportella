package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FemaleEducationMapOnlyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	/**
	 * If a record has cumulative female educational info
	 * about the USA then take the average change per 
	 * year. 
	 * (1)Get the earliest percentage from 2000 to 
	 * most recent percentage over the number of years between 
	 * them. 
	 * Map this value to the key of the United States
	 * and the educational attainment indicator. 
	 * 
	 * Assumptions: May want more descriptive and granular 
	 * statistics than the summary data outputed by the 
	 * alternative mapper for female education. 
	 * Using the formula (1) find the average increase 
	 * for each form of cumulative educational data. 
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
		String indicator1 = "SE";
		String indicator2 = "CUAT";
		String indicator3 = "FE";
		String countryCode = "USA";
		int indicatorIndex = 3;
		int countryCodeIndex = 1;

		String outputKey = line[0].substring(1);		

		int initialYearIndex = 44;		
		int initialYear = 2000;

		int mostRecentYearIndex = line.length-1;
		int mostRecentYear = 2016;

		Double mostRecentYearVal = null, initialYearVal = null;
		String strMostRecentYearVal, strInitialYearVal = "";

		Double averageChange = 0.0;

		if (line[countryCodeIndex].equals(countryCode)){
			String indicatorCode = line[indicatorIndex];
			if(indicatorCode.contains(indicator1) && indicatorCode.contains(indicator2) && indicatorCode.contains(indicator3)){
				do{
					strInitialYearVal = line[initialYearIndex];
					try{
						initialYearVal = Double.parseDouble(strInitialYearVal);
						if (initialYearVal <= 0) throw new NumberFormatException();
						break;
					}catch(NumberFormatException ex){
						initialYearIndex ++;
						initialYear++;
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
					outputKey = " "+line[countryCodeIndex+1];
					context.write(new Text(outputKey), new DoubleWritable(averageChange));
				}
			}
		}
	}

}