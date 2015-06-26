package org.krisna.customRecordReader;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CustomRecReaderMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
	public void map(LongWritable key, Text value,Context context) throws java.io.IOException ,InterruptedException
	{
	    String lines = value.toString();
	    String []lineArr = lines.split("\n");
	    int lcount = lineArr.length;
	    context.write(new Text(new Integer(lcount).toString()),new IntWritable(1));
	 }
}


