package org.sbi.latest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

//import org.krisna.TopNodes.StaticFields;

public class ErrorChk {
	public static Map<String, Integer> SingleNodeMap = new HashMap<String, Integer>();

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			line=line.replace("Products, Services and Fund Transfer Menu","Products and Services menu");
			String[] lineSplit = line.split(",");
			if (lineSplit.length == 4 && !lineSplit[1].equals("undefined")) {
				Text mapKey = new Text(lineSplit[1]);
				Text mapVal = new Text(lineSplit[2]);
				context.write(mapKey, mapVal);
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, NullWritable> {

		@Override
		public void run(Context context) throws IOException,
				InterruptedException {
			setup(context);
			try {
				while (context.nextKey()) {
					reduce(context.getCurrentKey(), context.getValues(),
							context);
				}
			} finally {
				Text Test = new Text("Final");
				ArrayList<Text> Val = new ArrayList<Text>();
				Val.add(Test);
				Val.add(new Text("value"));
				reduce(Test, Val, context);
				cleanup(context);
			}
		}

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			ArrayList<String> MenuList = new ArrayList<String>();
			MenuList.add("Account Information");
			MenuList.add("Debit Card Menu");
			MenuList.add("Internet & Mobile Banking Menu");
			MenuList.add("Products and Services Menu");
			MenuList.add("Products and Services menu");
			MenuList.add("Lodge a Complaint");
			MenuList.add("Pension Accounts");           
			
			List<String> CallMenuList = new ArrayList<String>();			
			List<String> ErrorList = new ArrayList<String>();
			for (Text val : values) {
				if (MenuList.contains(val.toString())) {
					if(!CallMenuList.contains(val.toString()))
					{
						CallMenuList.add(val.toString());
					}
					
				}
				String MyVal = val.toString().toLowerCase();
				if (MyVal.contains("standard") || MyVal.contains("error") || MyVal.contains("invalid")|| MyVal.contains("tries")|| MyVal.contains("exceeded")) {
					ErrorList.add(MyVal);
				}
			}
			if(CallMenuList.size()==0)
			{
				for (String val : ErrorList) {
						Integer count = SingleNodeMap.get(val+"~Zero");
						SingleNodeMap.put(val+"~Zero", (count == null) ? 1 : count + 1);
				}
			}
			else if(CallMenuList.size()==1)
			{
				for (String val : ErrorList) {
						Integer count = SingleNodeMap.get(val+"~Single");
						SingleNodeMap.put(val+"~Single", (count == null) ? 1 : count + 1);
				}
			}
			else if(CallMenuList.size()>1)
			{
				for (String val : ErrorList) {
						Integer count = SingleNodeMap.get(val+"~Multiple");
						SingleNodeMap.put(val+"~Multiple", (count == null) ? 1 : count+1);
				}
			}
			ErrorList.clear();
			if (key.toString().equals("Final")) {
				for (Entry<String, Integer> entry1 : SingleNodeMap.entrySet()) {
					context.write(new Text(entry1.getKey()+"="+entry1.getValue()), NullWritable.get());
				}
			}
		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: Wrong ArguementsCount <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Single Menu and Productive node count");
		job.setJobName("Error Check");

		job.setJarByClass(ErrorChk.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		//job.waitForCompletion(true);
		job.submit();
	}
}
