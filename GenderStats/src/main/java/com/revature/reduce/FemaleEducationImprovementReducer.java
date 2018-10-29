package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FemaleEducationImprovementReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
/**
 * Takes the average of the averages 
 * 
 * Assumptions: The average of the averages of increase in educational attainment 
 * is a useful summary statistic of how the improvement is going in all forms of 
 * education per year. It gives a snapshot view.
 * 
 * @param key United States 
 * @param values List of averages per year of cumulative educational attainment
 * @param context reference to output 
 * 
 * @return
 */
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		Double sum = 0.0;
		Double numOfValues = 0.0;
		
		for (DoubleWritable value : values) {
			sum += value.get();
			numOfValues++;
		}
		numOfValues = (numOfValues == 0.0) ? 1.0 :numOfValues;
		Double aveOverallImp = sum/numOfValues;
		context.write(key, new DoubleWritable(aveOverallImp));
	}
}