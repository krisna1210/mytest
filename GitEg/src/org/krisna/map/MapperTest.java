package org.krisna.test;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;


public class MapperTest {
	MapDriver<LongWritable, Text, Text,IntWritable> mapDriver;
	@Before
	public void setup() {
		DataMapper mapper = new DataMapper();
		mapDriver = MapDriver.newMapDriver(mapper);
		
	}
	
	@Test
	public void TestMapper() throws IOException {
		mapDriver.withInput(new LongWritable(),new Text("hadoop;great;work;done"));
		mapDriver.withOutput(new Text("hadoop"),new IntWritable(1));
		mapDriver.runTest();
	}

}
