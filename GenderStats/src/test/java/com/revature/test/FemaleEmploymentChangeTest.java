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
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.FemaleEmploymentChangeMapper;
import com.revature.reduce.FemaleEmploymentChangeReducer;

public class FemaleEmploymentChangeTest{
	private static String path = "femaleEmpChange.csv";
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, DoubleWritable> mapReduceDriver;
	private static String outputKey = "Percent change in female labor force participation from 2000 to 2016";
	private static String intermediateOutput = "52.02061329862,51.9473977490027,51.8963649802153,51.8555857173774,51.8376490048096,51.8726002200367,51.4903546908444,51.1202787113567,50.7513504355734,50.3921674878606,49.9751603357265,49.7697230902693,49.6429885535374,49.6114967415687,49.6049188556052,49.5663674221347,49.5028240856305";

	@Before
	public void setUp() {


		Mapper<LongWritable, Text, Text, Text> mapper = new FemaleEmploymentChangeMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);


		FemaleEmploymentChangeReducer reducer = new FemaleEmploymentChangeReducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, DoubleWritable>();
		reduceDriver.setReducer(reducer);


		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, DoubleWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}


	@Test
	public void testMapper() {

		try (BufferedReader br =
				new BufferedReader(new FileReader(path))){
			String line = br.readLine();
			while (line != null) {
				mapDriver.setInput(new LongWritable(1), new Text(line));
				mapDriver.withOutput(new Text("Percent change in female labor force participation from 2000 to 2016"), new Text(intermediateOutput));
				mapDriver.runTest();
				line=br.readLine();
			}
		}catch(IOException ex) {
			ex.printStackTrace();
		}
	}



	@Test
	public void testReducer() {

		List<Text> values = new ArrayList<Text>();
		values.add(new Text(intermediateOutput));

		reduceDriver.withInput(new Text(outputKey), values);
		Double recent = 49.5028240856305;
		Double yr2000= 52.02061329862;
		Double change = ((recent-yr2000)/yr2000)*100;
		reduceDriver.withOutput(new Text(outputKey), new DoubleWritable(change));


		reduceDriver.runTest();
	}


	@Test
	public void testMapReduce() {
		try (BufferedReader br =
				new BufferedReader(new FileReader(path))){
			String line = br.readLine();
			while (line != null) {
				mapReduceDriver.addInput(new LongWritable(1), new Text(line));
				line=br.readLine();
			}
			Double recent = 49.5028240856305;
			Double yr2000= 52.02061329862;
			Double change = ((recent-yr2000)/yr2000)*100;
			mapReduceDriver.addOutput(new Text(outputKey), new DoubleWritable(change));
			mapReduceDriver.runTest();

		}catch(IOException ex) {
			ex.printStackTrace();
		}
	}



}
