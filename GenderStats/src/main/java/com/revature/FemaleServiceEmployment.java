package com.revature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.revature.map.FemaleServiceEmploymentMapper;
import com.revature.reduce.FemaleServiceEmploymentIncreasingReducer;
import com.revature.reduce.FemaleServiceEmploymentReducer;

/**
 * Female Service Employment is the driving class for the 
 * Female Service Employment mapper,  combiner(intermediate reducer), and reducer. 
 * It creates the female employment job. It reads in 
 * the input csv from the command line and the desired output path.  
 * 
 * @author Bryn Portella
 *
 */

public class FemaleServiceEmployment extends Configured implements Tool{
	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.printf("Usage: WordCountDriver <input dir> <output dir>\n");
			return -1;
		}

		Job job = new Job(getConf());
				
		job.setJarByClass(FemaleServiceEmployment.class);
		
		job.setJobName("Female Service Employment");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(FemaleServiceEmploymentMapper.class);
		job.setReducerClass(FemaleServiceEmploymentIncreasingReducer.class);
		
		job.setCombinerClass(FemaleServiceEmploymentReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		if (job.getCombinerClass() == null) {
			throw new Exception("Combiner not set");
		}

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new FemaleServiceEmployment(), args);
		System.exit(exitCode);
	}
}

