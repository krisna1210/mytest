package org.krisna.multipleMR;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class JobDriver {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: UniqueUserCount <in> <out>");
			System.exit(2);
		}

		Path tmpout = new Path(otherArgs[1] + "_tmp");
		FileSystem.get(new Configuration()).delete(tmpout, true);
		Path finalout = new Path(otherArgs[1]);
		 Job job = Job.getInstance(conf, "Production and Non Production count");

//		Job job = new Job(conf, "StackOverflow Unique User Count");
		job.setJarByClass(JobDriver.class);
		
		job.setMapperClass(org.krisna.multipleMR.MyMapper1.FirstMapper.class);
		job.setReducerClass(org.krisna.multipleMR.Myreducer1.FirstReducer.class);

		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, tmpout);

		boolean exitCode = job.waitForCompletion(true);
		if (exitCode) {
			 Job job1 = Job.getInstance(conf, "Production and Non Production count");
			//job = new Job(conf, "Stack Overflow Unique User Count");
			job1.setJarByClass(JobDriver.class);
			
			job1.setMapperClass(MyMapper2.SecondMapper.class);
			
			job1.setReducerClass(MyReducer2.SecondReducer.class);
			
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);
			
			job1.setOutputFormatClass(TextOutputFormat.class);
			job1.setInputFormatClass(TextInputFormat.class);
			
			FileInputFormat.addInputPath(job1, tmpout);
			FileOutputFormat.setOutputPath(job1, finalout);
			exitCode = job1.waitForCompletion(true);
		}

		System.exit(exitCode ? 0 : 1);
	}

}
