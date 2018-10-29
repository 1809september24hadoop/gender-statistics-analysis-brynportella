package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.FemaleEducationMapOnlyMapper;
import com.revature.reduce.FemaleEducationImprovementReducer;
/**
 * FemaleEducationMapOnly is the driving class for the 
 * FemaleEducationMapOnlyMapper, which outputs more descriptive 
 * intermediate output with the kinds of educational attainment
 * and their changes. 
 * It reads in the input csv from the command line and the desired output path.  
 * 
 * @author Bryn Portella
 *
 */



public class FemaleEducationMapOnly {
	public static void main(String[] args) throws Exception{
		if (args.length != 2){
			System.out.printf(
					"Usage: Female Graduate <input dir> <output dir>");
			System.exit(-1);
		}
		
		Job job = new Job();
		
		job.setJarByClass(FemaleEducationImprovement.class);
		
		job.setJobName("Female Education Improvement: Different Categories");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(FemaleEducationMapOnlyMapper.class);
		job.setReducerClass(FemaleEducationImprovementReducer.class);
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		boolean success = job.waitForCompletion(true);
		
		System.exit(success ? 0:1);
	}
}
