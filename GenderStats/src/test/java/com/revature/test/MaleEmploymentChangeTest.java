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

import com.revature.map.MaleEmploymentChangeMapper;
import com.revature.reduce.MaleEmploymentChangeReducer;


public class MaleEmploymentChangeTest {

  private static String path = "maleEmpChange.csv";
  private MapDriver<LongWritable, Text, Text, Text> mapDriver;
  private ReduceDriver<Text, Text, Text, DoubleWritable> reduceDriver;
  private MapReduceDriver<LongWritable, Text, Text, Text, Text, DoubleWritable> mapReduceDriver;
  private static String outputKey = "Percent change in male labor force participation from 2000 to 2016";
  private static String intermediateOutput = "78.7845573697158,78.4778692138528,78.1723954366242,77.9152579970048,77.7611802616711,77.648002262521,77.4004508040747,77.1831616870868,77.0292742805193,76.7570004729681,76.4963699811064,76.3648142978603,76.2994136837516,76.2196138012021,76.1555671063887,76.187067221201,76.1697053427216";
  
  @Before
  public void setUp() {

    
    Mapper<LongWritable, Text, Text, Text> mapper = new MaleEmploymentChangeMapper();
    mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
    mapDriver.setMapper(mapper);

   
    MaleEmploymentChangeReducer reducer = new MaleEmploymentChangeReducer();
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
			    mapDriver.withOutput(new Text("Percent change in male labor force participation from 2000 to 2016"), new Text(intermediateOutput));
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
    Double recent = 76.1697053427216;
    Double yr2000= 78.7845573697158;
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
		    Double recent = 76.1697053427216;
		    Double yr2000= 78.7845573697158;
		    Double change = ((recent-yr2000)/yr2000)*100;
		    mapReduceDriver.addOutput(new Text(outputKey), new DoubleWritable(change));
			mapReduceDriver.runTest();

		}catch(IOException ex) {
			ex.printStackTrace();
		}
  }

  
  
}