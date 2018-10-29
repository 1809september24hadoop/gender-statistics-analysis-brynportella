package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FemaleServiceEmploymentIncreasingReducer extends Reducer<Text, Text, Text, DoubleWritable> {
	/**
	 * Ouputs countries where percentage of female labor force in 
	 * services is increasing 
	 *  
	 * Assumptions: Positive average change indicates the percentage 
	 * is increasing 
	 * 
	 * @param key Country
	 * @param values ave change in percentage of female labor force in services
	 * for each country 
	 * @param context  
	 * @return
	 */

	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, DoubleWritable>.Context context)
					throws IOException, InterruptedException {
		int isIncreasing = 0;		

		for (Text value: values){
			Double aveChange = Double.parseDouble(value.toString());
			if (aveChange > 0) context.write(new Text(key), new DoubleWritable(aveChange));
		
		}
	}

}
