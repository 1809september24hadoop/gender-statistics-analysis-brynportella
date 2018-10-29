package com.revature;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.MaleEmploymentChangeMapper;
import com.revature.reduce.MaleEmpChangeMapRedReducer;


public class MaleEmpChangeMapReduce {
	public static void main(String[] args) throws Exception{
		if (args.length != 2){
			System.out.printf(
					"Usage: MaleEmploymentChange <input dir> <output dir>");
			System.exit(-1);
		}
		
		Job job = new Job();
		
		job.setJarByClass(MaleEmpChangeMapReduce.class);
		
		job.setJobName("Percent Male Employment Change");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(MaleEmploymentChangeMapper.class);
		job.setReducerClass(MaleEmpChangeMapRedReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		boolean success = job.waitForCompletion(true);
		
		System.exit(success ? 0:1);
	}
}