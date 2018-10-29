package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.FemaleEducationImprovementMapper;
import com.revature.reduce.FemaleEducationImprovementReducer;

/**
 * FemaleEducationImprovement is the driving class for the 
 * Female Education Improvement Mapper and Reducer. 
 * It creates the female education job. It reads in 
 * the input csv from the command line and the desired output path.  
 * 
 * @author Bryn Portella
 *
 */

public class FemaleEducationImprovement {
	public static void main(String[] args) throws Exception{
		if (args.length != 2){
			System.out.printf(
					"Usage: FemaleEducationImprovement <input dir> <output dir>");
			System.exit(-1);
		}
		
		Job job = new Job();
		
		job.setJarByClass(FemaleEducationImprovement.class);
		
		job.setJobName("Female Education Improvement");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(FemaleEducationImprovementMapper.class);
		job.setReducerClass(FemaleEducationImprovementReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		boolean success = job.waitForCompletion(true);
		
		System.exit(success ? 0:1);
	}
}
