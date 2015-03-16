package org.sbi.latest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
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

public class CustPsyche {
	public static long ReduceCnt = 0;
	public static Map<String, Integer> SingleNodeMap = new HashMap<String, Integer>();
	public static Map<String, Integer> SingleMenuMap = new HashMap<String, Integer>();
	public static Map<String, Integer> Cnt = new HashMap<String, Integer>();
	
	//public static Map<String, Integer> MultiNodeMap = new HashMap<String, Integer>();
	//public static Map<String, Integer> NodeTravelMap = new TreeMap<String, Integer>();

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
			//String PreKey = "test";
			ArrayList<String> ProductiveNodesList = new ArrayList<String>();
			ArrayList<String> MenuList = new ArrayList<String>();
			MenuList.add("Account Information");
			MenuList.add("Debit Card Menu");
			MenuList.add("Internet & Mobile Banking Menu");
			MenuList.add("Products and Services Menu");
			MenuList.add("Products and Services menu");
			MenuList.add("Lodge a Complaint");
			MenuList.add("Pension Accounts");           
			//context.write(new Text("Key "+key), NullWritable.get());
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

			
			
			List<String> NodeList = new ArrayList<String>();
			List<String> CallMenuList = new ArrayList<String>();			
			for (Text val : values) {
				if (MenuList.contains(val.toString())) {
					if(!CallMenuList.contains(val.toString()))
					{
						CallMenuList.add(val.toString());
					}
				}
				if (ProductiveNodesList.contains(val.toString())) {
					if(!NodeList.contains(val.toString()))
					{
						NodeList.add(val.toString());
					}
				}
			}
			Map<String, Integer> NodeMap = new HashMap<String, Integer>();
			Map<String, Integer> MenuMap = new HashMap<String, Integer>();
			//context.write(new Text("size "+CallMenuList.size()), NullWritable.get());
			if(NodeList.size()==1)
			{
				for (String temp : NodeList) {
					if(temp.equals("Last 5 Transactions on SMS")) {  
						context.write(new Text("Key "+key), NullWritable.get());
					}
					Integer count = NodeMap.get(temp);
					NodeMap.put(temp, (count == null) ? 1 : count + 1);
				}
			}
			
			
			String NodeTemp="Zero Node";
			if(NodeList.size()==1)
			{
				NodeTemp="Single Node";
			}
			else if(NodeList.size()>1)
			{
				NodeTemp="Multi node";
			}
			Integer NodeCount = Cnt.get(NodeTemp);
			Cnt.put(NodeTemp, (NodeCount == null) ? 1 : NodeCount + 1);
			String MenuTemp="Zero Menu";
			if(CallMenuList.size()==1)
			{
				MenuTemp="Single Menu";
			}
			else if(CallMenuList.size()>1)
			{
				MenuTemp="Multi Menu";
			}
			
			Integer MenuCount = Cnt.get(MenuTemp);
			Cnt.put(MenuTemp, (MenuCount == null) ? 1 : MenuCount + 1);

			NodeList.clear();
			if(CallMenuList.size()==1)
			{
				for (String temp : CallMenuList) {
					Integer count = MenuMap.get(temp);
					MenuMap.put(temp, (count == null) ? 1 : count + 1);
				}
			}
			CallMenuList.clear();

			
			
			for (Entry<String, Integer> entry : NodeMap.entrySet()) {
				if (entry.getValue() == 1) {
					Integer count = SingleNodeMap.get(entry.getKey());
					SingleNodeMap.put(entry.getKey(), (count == null) ? 1
							: count + 1);
				}/* else {
					Integer count = MultiNodeMap.get(entry.getKey());
					MultiNodeMap.put(entry.getKey(), (count == null) ? 1
							: count + 1);
				}*/
			}
			NodeMap.clear();
			for (Entry<String, Integer> entry : MenuMap.entrySet()) {
				if (entry.getValue() == 1) {
					Integer count = SingleMenuMap.get(entry.getKey());
					SingleMenuMap.put(entry.getKey(), (count == null) ? 1
							: count + 1);
				}/* else {
					Integer count = MultiNodeMap.get(entry.getKey());
					MultiNodeMap.put(entry.getKey(), (count == null) ? 1
							: count + 1);
				}*/
			}
			MenuMap.clear();

			if (!key.toString().equals("Final"))	ReduceCnt++;
			
			if (key.toString().equals("Final")) {
				
				for (Entry<String, Integer> entry1 : Cnt.entrySet()) {
					context.write(new Text(entry1.getKey()+"\t"+entry1.getValue()), NullWritable.get());
				}
				long SingleMenuSum = 0;
				for (Entry<String, Integer> entry1 : SingleMenuMap.entrySet()) {
					SingleMenuSum+=entry1.getValue();
				}
				
				context.write(new Text("Total Calls\t "+ReduceCnt), NullWritable.get());
				context.write(new Text("\nCustomers used single menu throughout the call\t "+SingleMenuSum), NullWritable.get());
				for (Entry<String, Integer> entry1 : SingleMenuMap.entrySet()) {
					context.write(new Text(entry1.getKey()+"\t"+entry1.getValue()), NullWritable.get());
				}
				long SingleNodeSum = 0;
				for (Entry<String, Integer> entry1 : SingleNodeMap.entrySet()) {
					SingleNodeSum+=entry1.getValue();
				}
				context.write(new Text("\nCustomers used single Node throughout the call \t"+SingleNodeSum), NullWritable.get());
				for (Entry<String, Integer> entry1 : SingleNodeMap.entrySet()) {
					context.write(new Text(entry1.getKey()+"\t"+entry1.getValue()), NullWritable.get());
				}
				/*
				context.write(new Text("Customers used multiple menu throughout the call"), new LongWritable());
				for (Entry<String, Integer> entry2 : MultiNodeMap.entrySet()) {
					context.write(new Text(entry2.getKey()), new LongWritable(
							entry2.getValue()));
				}
				*/
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
		job.setJobName("Top Five Nodes");

		job.setJarByClass(CustPsyche.class);
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

class ValueComparator implements Comparator<String> {

	Map<String, Long> base;

	public ValueComparator(Map<String, Long> base) {
		this.base = base;
	}

	// Note: this comparator imposes orderings that are inconsistent with
	// equals.
	public int compare(String a, String b) {
		if (base.get(a) >= base.get(b)) {
			return -1;
		} else {
			return 1;
		} // returning 0 would merge keys
	}
}
