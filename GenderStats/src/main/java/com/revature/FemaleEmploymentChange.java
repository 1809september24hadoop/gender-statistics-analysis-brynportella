package com.revature;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.FemaleEmploymentChangeMapper;
import com.revature.reduce.FemaleEmploymentChangeReducer;

/**
 * Driver for the Map Reduce 
 * of female employment change. 
 * 
 * Creates an map reduce Job and 
 * reads csv file input from the 
 * command line.
 * 
 * @author Bryn Portella
 * 
 */

public class FemaleEmploymentChange {
	public static void main(String[] args) throws Exception{
		if (args.length != 2){
			System.out.printf(
					"Usage: FemaleEmploymentChange <input dir> <output dir>");
			System.exit(-1);
		}
		
		Job job = new Job();
		
		job.setJarByClass(FemaleEmploymentChange.class);
		
		job.setJobName("Percent Female Employment Change");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(FemaleEmploymentChangeMapper.class);
		job.setReducerClass(FemaleEmploymentChangeReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		boolean success = job.waitForCompletion(true);
		
		System.exit(success ? 0:1);
	}
}