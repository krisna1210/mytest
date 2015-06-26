package org.krisna.customPartioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class CustomPartitionerDriver extends Configured implements Tool {
	
	public static void main(String args[]) throws Exception{
		Configuration configuration = new Configuration();
	    ToolRunner.run(configuration, new CustomPartitionerDriver(),args);
	}
	
	 @Override
	  public int run(String[] arg0) throws Exception {
	    Job job = Job.getInstance(getConf());
	    job.setJarByClass(CustomPartitionerDriver.class);
		job.setJobName("Custom Partitioner Wordcount");
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);

	    job.setMapperClass(CustomPartitionerMapper.class);
	    job.setReducerClass(CustomPartitionerReducer.class);
	    job.setPartitionerClass(CustomParttioner.class);
	    job.setNumReduceTasks(27);
	    //Delete output filepath if already exists
		FileSystem fs = FileSystem.newInstance(getConf());
		

		if (fs.exists(new Path(arg0[1]))) {
			fs.delete(new Path(arg0[1]), true);
		}

	    FileInputFormat.addInputPath(job, new Path(arg0[0]));
	    FileOutputFormat.setOutputPath(job,new Path(arg0[1]));
	    job.submit();
	    int rc = (job.waitForCompletion(true)?1:0);
	    return rc;
	}

}
