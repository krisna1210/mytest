package org.krisna.TopNodes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
//import org.krisna.TopNodes.StaticFields;



public class ProdCalls {
	public static long ReduceCnt=1;
	public static long MultiNodeCnt=0;
	public static HashSet<String> nodesList=new HashSet<String>();	
	public static Map<String, Long> ProdNodes = new HashMap<String, Long>();
	public static Map<String, Long> NonProdNodes = new HashMap<String, Long>();
	public static long TotalProdCnt = 0;
	public static long TotalNonProdCnt = 0;
	static enum TopNodes { UniqueID,TotalRec }
	public static long mapperCounter;
	public static long mapperCounter1;

	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			ArrayList<String> ProductiveNodesList = new ArrayList<String>();
			ProductiveNodesList.add("Car Loans - Rates Played");
			ProductiveNodesList.add("Domestic Deposits - Rates Played");
			ProductiveNodesList.add("Educational Loans - Rates Played");
			ProductiveNodesList.add("FCNR Deposits - Rates Played");
			ProductiveNodesList.add("FOREX Rates Played");
			ProductiveNodesList.add("Gold Rate Played");
			ProductiveNodesList.add("Home Loan - Rates Played");
			ProductiveNodesList.add("Last 5 Transactions on SMS");
			ProductiveNodesList.add("Mobile Number Registered");
			ProductiveNodesList.add("Mobile Number De-registered");
			ProductiveNodesList.add("NRE Rates - Played");
			ProductiveNodesList.add("OTP Generated");
			ProductiveNodesList.add("Quick Balance");
			ProductiveNodesList.add("Details Sent on SMS");
			String line = value.toString();
	    	String[] lineSplit = line.split(",");
	    	Text Val = new Text(lineSplit[1]);
	    	if (ProductiveNodesList.contains(lineSplit[2])) {
	    		context.write(Val,new IntWritable(1));
			}
	    	else context.write(Val,new IntWritable(0));
	    	nodesList.add(lineSplit[1]);
	    	context.getCounter(TopNodes.UniqueID).setValue(nodesList.size());
	    	context.getCounter(TopNodes.TotalRec).increment(1);
	    	
	    }
	      }

	
	public static class Reduce extends Reducer<Text, IntWritable, Text, NullWritable> {
		
		@Override
		 public void run(Context context) throws IOException, InterruptedException {
			setup(context);
			try {
				while (context.nextKey()) {
					reduce(context.getCurrentKey(), context.getValues(), context);
					// If a back up store is used, reset it
					Iterator<IntWritable> iter = context.getValues().iterator();
					if(iter instanceof ReduceContext.ValueIterator) {
							((ReduceContext.ValueIterator<IntWritable>)iter).resetBackupStore();        
					}
				}
			} finally {
				Text Test = new Text("Final");
				ArrayList<IntWritable> Val= new ArrayList<IntWritable>();
				Val.add(new IntWritable(1));
				reduce(Test, Val, context);
				cleanup(context);
			}
		}
		
		public void reduce(Text key,  Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			Long sum = (long) 0;
			for (IntWritable val : values) {
				sum += (val.get());
			}
			if (sum>1) {
				MultiNodeCnt++;					
			}

			ReduceCnt++;
			if(key.toString().equals("Final"))
			{
				context.write(new Text("Same call multiple self service nodes"),NullWritable.get());
				context.write(new Text("Total no.of Calls with multiple Self Service nodes : "+MultiNodeCnt+""),NullWritable.get());
				context.write(new Text("Total no.of Calls with Unique UCID : "+mapperCounter),NullWritable.get());
				double ProdPercent = (((double) MultiNodeCnt)/(double) mapperCounter);
				String result = String.format("%.2f", ProdPercent);
				context.write(new Text("Avg travel between Self service nodes : "+result+"%"),NullWritable.get());
				
				context.write(new Text("\nAverage travell between nodes"),NullWritable.get());
				context.write(new Text("Total no.of nodes (Total records) : "+mapperCounter1),NullWritable.get());
				context.write(new Text("Total no.of Calls with Unique UCID : "+mapperCounter),NullWritable.get());
				double Prod1Percent = (((double) mapperCounter1)/(double) mapperCounter);
				String result1 = String.format("%.2f", Prod1Percent);
				context.write(new Text("Avg travel between nodes : "+result1+"%"),NullWritable.get());
			}
		}

	}
			
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: UniqueUserCount <in> <out>");
			System.exit(2);
		}
		 Job job = Job.getInstance(conf, "Multiple Productive Calls");
		 job.setJobName("Multiple Productive Calls");

		job.setJarByClass(ProdCalls.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		job.waitForCompletion(true);
	}
}