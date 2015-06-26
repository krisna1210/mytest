package org.krisna.test;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataMapper extends Mapper<LongWritable, Text, Text,IntWritable> {
	private Text Status = new Text();
	private final static IntWritable  addOne= new IntWritable(1);
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split(";");
		if(line[0].equals("hadoop")) {
			Status.set(line[0]);
			context.write(Status, addOne);
		}
	}

}
