package org.krisna.customRecordReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CustomRecReaderDriver extends Configured implements Tool{
	
	  public static void main(String[] args) throws Exception {
		    Configuration configuration = new Configuration();
		    ToolRunner.run(configuration, new CustomRecReaderDriver(),args);
		  }
		 
		  @Override
		  public int run(String[] arg0) throws Exception {
			Job job = Job.getInstance(getConf());
		    
			job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    
		    job.setJarByClass(CustomRecReaderDriver.class);
		    
		    job.setMapperClass(CustomRecReaderMapper.class);
		    job.setReducerClass(CustomRecReaderReducer.class);
		    
		    job.setInputFormatClass(NLinesInputFormat.class);
		    
		    FileInputFormat.addInputPath(job, new Path(arg0[0]));
		    FileOutputFormat.setOutputPath(job,new Path(arg0[1]));
		    job.submit();
		    int rc = (job.waitForCompletion(true)?1:0);
		    return rc;
		}

}
