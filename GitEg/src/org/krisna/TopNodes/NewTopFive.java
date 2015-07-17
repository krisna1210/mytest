package org.krisna.TopNodes;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
//import org.krisna.TopNodes.StaticFields;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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



public class NewTopFive {
	public static long ReduceCnt=1;
	public static HashSet<String> nodesList=new HashSet<String>();	
	public static Map<String, Long> ProdNodes = new HashMap<String, Long>();
	public static Map<String, Long> NonProdNodes = new HashMap<String, Long>();
	public static long TotalProdCnt = 0;
	public static long TotalNonProdCnt = 0;

	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	  String line = value.toString();
	    	  String[] lineSplit = line.split(",");
	    	  Text Val = new Text(lineSplit[2]);
	    	  nodesList.add(lineSplit[2]);
	    	  context.write(Val,new IntWritable(1));
	    	  }
	      }

	
	public static class Reduce extends Reducer<Text, IntWritable, Text, LongWritable> {
		
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
    	Long sum = (long) 0;

    	for (IntWritable val : values) {
			sum = (val.get()) + sum;
	      }
			if (ProductiveNodesList.contains(key.toString())) {
			ProdNodes.put(key.toString(),sum);					
			TotalProdCnt=TotalProdCnt+sum;
			}
			else {
				NonProdNodes.put(key.toString(),sum);
				TotalNonProdCnt=TotalNonProdCnt+sum;
			}
			ProductiveNodesList.clear();
			if(key.toString().equals("Final"))
			{
				ValueComparator bvc = new ValueComparator(ProdNodes);
				ValueComparator bvc1 = new ValueComparator(NonProdNodes);
	
				Map<String, Long> ProdTreeMap = new TreeMap<String, Long>(bvc);
				Map<String, Long> NonProdTreeMap = new TreeMap<String, Long>(bvc1);
	
				ProdTreeMap.putAll(ProdNodes);
				NonProdTreeMap.putAll(NonProdNodes);
				context.write(new Text("Total number of Productive Nodes\t\t"),new LongWritable(TotalProdCnt));
				for (Entry<String, Long> entry : ProdTreeMap.entrySet()) {
					context.write(new Text("\t" + entry.getKey() + "\t"), new LongWritable(entry.getValue()));
				}
				ProdTreeMap.clear();
				context.write(new Text("Total number of Non Productive Nodes\t\t"),new LongWritable(TotalNonProdCnt));
				System.gc();
				for (Entry<String, Long> entry : NonProdTreeMap.entrySet()) {
					context.write(new Text("\t" + entry.getKey() + "\t"), new LongWritable(entry.getValue()));
				}
				NonProdTreeMap.clear();
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
		 Job job = Job.getInstance(conf, "Production and Non Production count");
		 job.setJobName("Top Five Nodes");

		job.setJarByClass(NewTopFive.class);
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

class ValueComparator implements Comparator<String> {

    Map<String, Long> base;
    public ValueComparator(Map<String, Long> base) {
        this.base = base;
    }

   

	// Note: this comparator imposes orderings that are inconsistent with equals.    
    public int compare(String a, String b) {
        if (base.get(a) >= base.get(b)) {
            return -1;
        } else {
            return 1;
        } // returning 0 would merge keys
    }
}

