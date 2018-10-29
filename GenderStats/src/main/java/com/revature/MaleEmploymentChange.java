package com.revature;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.MaleEmploymentChangeMapper;
import com.revature.reduce.MaleEmploymentChangeReducer;
/**
 * Driver for the Map Reduce 
 * of male employment change. 
 * 
 * Creates an map reduce Job and 
 * reads csv file input from the 
 * command line.
 * 
 * 
 * @author Bryn Portella
 * 
 */

public class MaleEmploymentChange {
	public static void main(String[] args) throws Exception{
		if (args.length != 2){
			System.out.printf(
					"Usage: MaleEmploymentChange <input dir> <output dir>");
			System.exit(-1);
		}
		
		Job job = new Job();
		
		job.setJarByClass(MaleEmploymentChange.class);
		
		job.setJobName("Percent Male Employment Change");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(MaleEmploymentChangeMapper.class);
		job.setReducerClass(MaleEmploymentChangeReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		boolean success = job.waitForCompletion(true);
		
		System.exit(success ? 0:1);
	}
}