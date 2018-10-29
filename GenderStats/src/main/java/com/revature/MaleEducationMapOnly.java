package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.MaleEducationMapOnlyMapper;
import com.revature.reduce.MaleEducationImprovementReducer;

/**
 * MaleEducationMapOnly is the driving class for the 
 * MaleEducationMapOnlyMapper, which outputs more descriptive 
 * intermediate output with the kinds of educational attainment
 * and their changes. 
 * It reads in the input csv from the command line and the desired output path.  
 * 
 * @author Bryn Portella
 *
 */
public class MaleEducationMapOnly {
	public static void main(String[] args) throws Exception{
		if (args.length != 2){
			System.out.printf(
					"Usage: MaleEducationImprovement <input dir> <output dir>");
			System.exit(-1);
		}
		
		Job job = new Job();
		
		job.setJarByClass(MaleEducationImprovement.class);
		
		job.setJobName("Male Education Improvement");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(MaleEducationMapOnlyMapper.class);
		job.setReducerClass(MaleEducationImprovementReducer.class);
		
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		boolean success = job.waitForCompletion(true);
		
		System.exit(success ? 0:1);
	}
}
