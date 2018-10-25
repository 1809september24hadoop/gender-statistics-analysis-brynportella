package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FemaleEducationImprovementReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

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