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

import com.revature.map.FemaleGraduateMapper;

public class FemaleGraduateTest {

  private static String path = "femGradTest.csv";
  private MapDriver<LongWritable, Text, Text, Text> mapDriver;
  
  @Before
  public void setUp() {

    
    FemaleGraduateMapper mapper = new FemaleGraduateMapper();
    mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
    mapDriver.setMapper(mapper);
  }

  
  @Test
  public void testMapper() {

		try (BufferedReader br =
				new BufferedReader(new FileReader(path))){
			String line = br.readLine();
			long i = 1L;
			mapDriver.withInput(new LongWritable(i), new Text("CUAT"));
			while (line != null) {
				mapDriver.withInput(new LongWritable(i), new Text(line));
				i++;
				line = br.readLine();
			}
		    mapDriver.withOutput(new Text("Mali 2015"), new Text("3.48197"));
			mapDriver.runTest();
		}catch(IOException ex) {
			ex.printStackTrace();
		}
  }
  
}