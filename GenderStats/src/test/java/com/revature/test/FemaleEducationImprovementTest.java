package com.revature.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.FemaleEducationImprovementMapper;
import com.revature.reduce.FemaleEducationImprovementReducer;

public class FemaleEducationImprovementTest {

  private static String path = "femaleEdImpTester.csv";
  private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
//  private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
//  private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;
//
//  
  @Before
  public void setUp() {

    
    FemaleEducationImprovementMapper mapper = new FemaleEducationImprovementMapper();
    mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
    mapDriver.setMapper(mapper);

//   
//    FemaleEducationImprovementReducer reducer = new FemaleEducationImprovementReducer();
//    reduceDriver = new ReduceDriver<Text, DoubleWritable, Text, DoubleWritable>();
//    reduceDriver.setReducer(reducer);
//
//
//    mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable>();
//    mapReduceDriver.setMapper(mapper);
//    mapReduceDriver.setReducer(reducer);
  }

  
  @Test
  public void testMapper() {

		try (BufferedReader br =
				new BufferedReader(new FileReader(path))){
			String line = br.readLine();
			while (line != null) {
//				System.out.println(line);
//				String[] l =line.substring(1, line.length()-2).split("\",\"");
//				int index = 0;
//				int year = 1960;
//				for(String w : l){
//					if (index>3){
//					System.out.println(index+" "+year+": $$"+w+"$$");
//					year++;
//					}else{
//					System.out.println(index+" "+": $$"+w+"$$");
//					}
//					index++;
//				}
				mapDriver.setInput(new LongWritable(1), new Text(line));
				Double ave = (98.76783-98.59983)/11;
			    mapDriver.withOutput(new Text("United States"), new DoubleWritable(ave));
				mapDriver.runTest();
				line=br.readLine();
			}
		}catch(IOException ex) {
			ex.printStackTrace();
		}
  }


   
//  @Test
//  public void testReducer() {
//
//    List<DoubleWritable> values = new ArrayList<DoubleWritable>();
//    values.add(new DoubleWritable(98.0));
//    values.add(new DoubleWritable(32.687));
//
//    reduceDriver.withInput(new Text("USA"), values);
//    Double ave = (98.0+32.687)/2;
//   
//    reduceDriver.withOutput(new Text("USA"), new DoubleWritable(ave));
//
//  
//    reduceDriver.runTest();
//  }
//	
//
//  @Test
//  public void testMapReduce() {
//	  String path = "femaleEdImpTester.csv";
//		try (BufferedReader br =
//				new BufferedReader(new FileReader(path))){
//			String line = br.readLine();
//			while (line != null) {
//				mapReduceDriver.addInput(new LongWritable(1), new Text(line));
//				line=br.readLine();
//			}
//			Double ave = (98.76783-98.59983)/11;
//		    mapReduceDriver.addOutput(new Text("United States"), new DoubleWritable(ave));
//			mapReduceDriver.runTest();
//
//		}catch(IOException ex) {
//			ex.printStackTrace();
//		}
//  }
//
//  
  
}