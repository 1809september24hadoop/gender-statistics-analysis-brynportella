package com.revature.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.FemaleServiceEmploymentMapper;
import com.revature.reduce.FemaleServiceEmploymentIncreasingReducer;
import com.revature.reduce.FemaleServiceEmploymentReducer;


public class FemaleServiceEmploymentTest{
	private static String path = "femEmpServ.csv";
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> intermediateReducerDriver;
	private ReduceDriver<Text, Text, Text, DoubleWritable>  reduceDriver;
	private static String outputKey = "United States";
	private static String intermediateOutput = "86.9800033569336,87.8300018310547,88.7200012207031,89.0100021362305,89.3199996948242,89.6100006103516,89.7600021362305,89.8899993896484,90.3000030517578,91.2200012207031,91.370002746582,91.0500030517578,91,91.0599975585938,90.870002746582,90.8399963378906,90.9499969482422";
	@Before
	public void setUp() {


		Mapper<LongWritable, Text, Text, Text> mapper = new FemaleServiceEmploymentMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);


		FemaleServiceEmploymentReducer reducer = new FemaleServiceEmploymentReducer();
		intermediateReducerDriver = new ReduceDriver<Text, Text, Text, Text>();
		intermediateReducerDriver.setReducer(reducer);


		FemaleServiceEmploymentIncreasingReducer  reducer2 = new FemaleServiceEmploymentIncreasingReducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, DoubleWritable>();
		reduceDriver.setReducer(reducer2);
	}


	@Test
	public void testMapper() {

		try (BufferedReader br =
				new BufferedReader(new FileReader(path))){
			String line = br.readLine();
			while (line != null) {
				mapDriver.setInput(new LongWritable(1), new Text(line));
				mapDriver.withOutput(new Text(outputKey), new Text(intermediateOutput));
				mapDriver.runTest();
				line=br.readLine();
			}
		}catch(IOException ex) {
			ex.printStackTrace();
		}
	}
	
	
	@Test
	public void testReducerIntermediate(){
		List<Text> values = new ArrayList<Text>();
		values.add(new Text(intermediateOutput));
		intermediateReducerDriver.withInput(new Text(outputKey), values);
		Double earlyVal = 86.9800033569336;
		Double laterVal = 90.9499969482422;
		Double ave = (laterVal-earlyVal)/16;
		intermediateReducerDriver.withOutput(new Text(outputKey),new Text(ave.toString()));
	}



	@Test
	public void testReducer() {

		List<Text> values = new ArrayList<Text>();
		Double earlyVal = 86.9800033569336;
		Double laterVal = 90.9499969482422;
		Double ave = (laterVal-earlyVal)/16;
		values.add(new Text(ave.toString()));
		reduceDriver.withInput(new Text(outputKey), values);
		reduceDriver.withOutput(new Text(outputKey), new DoubleWritable(ave));
		reduceDriver.runTest();
	}




}
