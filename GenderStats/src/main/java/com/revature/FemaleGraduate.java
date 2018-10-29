package com.revature;

/**
 * Driver for first business question.
 * 
 * Creates a mapreduce job 
 * where the mapper filters 
 * out all countries with greater 
 * than 30% female graduation rate
 * and sets the amount of reducers 
 * to zero.
 * 
 * Reads csv input from the command
 * line.
 * 
 * @author Bryn Portella
 */

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.FemaleGraduateMapper;

public class FemaleGraduate {
	public static void main(String[] args) throws Exception{
		if (args.length != 2){
			System.out.printf(
					"Usage: Female Graduate <input dir> <output dir>");
			System.exit(-1);
		}
		
		Job job = new Job();
		
		job.setJarByClass(FemaleGraduate.class);
		
		job.setJobName("Female Graduation Rates");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(FemaleGraduateMapper.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		boolean success = job.waitForCompletion(true);
		
		System.exit(success ? 0:1);
	}
}
