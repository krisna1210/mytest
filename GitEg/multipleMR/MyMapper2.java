
package org.krisna.multipleMR;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MyMapper2 {
	public static class SecondMapper extends	Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] lineSplit = line.split("\\t");
			Text PorNP = new Text(lineSplit[0]);
			Text CountValue = new Text(lineSplit[1]);
			context.write(CountValue,PorNP);
		}
	}
}
