package org.krisna.multipleMR;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MyMapper1 {
	public static class FirstMapper extends	Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] lineSplit = line.split(",");
			Text UCID = new Text(lineSplit[1]);
			Text Activity = new Text(lineSplit[2]);
			context.write(UCID,Activity);
		}
	}
}
