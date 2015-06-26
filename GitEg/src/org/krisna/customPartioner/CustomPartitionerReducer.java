package org.krisna.customPartioner;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CustomPartitionerReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	private IntWritable totalWordCount = new IntWritable();

	@Override
	public void reduce(final Text key, final Iterable<IntWritable> values,final Context context) 
			throws IOException, InterruptedException {
		int count = 0;
	     for (IntWritable item : values)
	     {
	       count += item.get();
	     }
		totalWordCount.set(count);
		context.write(key, totalWordCount);
	}
}