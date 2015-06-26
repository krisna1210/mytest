package org.krisna.customRecordReader;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CustomRecReaderReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
   public void reduce(Text key, Iterable<IntWritable> list ,Context context) throws java.io.IOException ,InterruptedException
   {
     int count = 0;
     for (IntWritable item : list)
     {
       count += item.get();
     }
     context.write(key, new IntWritable(count));
   }
}