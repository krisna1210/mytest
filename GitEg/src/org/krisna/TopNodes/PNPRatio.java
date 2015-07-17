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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
//import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class PNPRatio {
	public static Short ReduceCnt=1;
	public static HashSet<String> nodesList=new HashSet<String>();	
	static Map<String, Long> ProdNodes = new HashMap<String, Long>();
	static Map<String, Long> NonProdNodes = new HashMap<String, Long>();
	static long TotalProdCnt = 0;
	static long TotalNonProdCnt = 0;

	public static class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

	      public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	    	  String line = value.toString();
	    	  String[] lineSplit = line.split(",");
	    	  Text Val = new Text(lineSplit[2]);
	    	  nodesList.add(lineSplit[2]);
	    	  output.collect(Val,new IntWritable(1));
	    	  }
	      }

	
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, LongWritable> {
	    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
	    	System.out.println(ReduceCnt+"red cnt = nodelist"+nodesList.size());
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
	    	long sum = 0;
			
			while (values.hasNext()) {
				sum += values.next().get();
			}
			if (ProductiveNodesList.contains(key.toString())) {
			ProdNodes.put(key.toString(),sum);					
			TotalProdCnt=TotalProdCnt+sum;
			output.collect(new Text(key+"~Productive"), new LongWritable(sum));
			
			}
			else {
				NonProdNodes.put(key.toString(),sum);
				TotalNonProdCnt=TotalNonProdCnt+sum;
				output.collect(new Text(key+"~NonProductive"), new LongWritable(sum));
			}

			ProductiveNodesList.clear();
			ReduceCnt++;
			System.gc();
			System.out.println(ReduceCnt+"--"+nodesList.size());
			if(ReduceCnt==nodesList.size())
			{
				ValueComparator bvc = new ValueComparator(ProdNodes);
				ValueComparator bvc1 = new ValueComparator(NonProdNodes);
	
				Map<String, Long> ProdTreeMap = new TreeMap<String, Long>(bvc);
				Map<String, Long> NonProdTreeMap = new TreeMap<String, Long>(bvc1);
	
				ProdTreeMap.putAll(ProdNodes);
				NonProdTreeMap.putAll(NonProdNodes);
				output.collect(new Text("Total number of Production\t\t"),new LongWritable(TotalProdCnt));
				for (Entry<String, Long> entry : ProdTreeMap.entrySet()) {
					output.collect(new Text("\t" + entry.getKey() + "\t"), new LongWritable(entry.getValue()));
				}
				ProdTreeMap.clear();
				output.collect(new Text("Total number of Non Production\t\t"),new LongWritable(TotalNonProdCnt));
				System.gc();
				for (Entry<String, Long> entry : NonProdTreeMap.entrySet()) {
					output.collect(new Text("\t" + entry.getKey() + "\t"), new LongWritable(entry.getValue()));
				}
				NonProdTreeMap.clear();
			}
		}
	}
			
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(PNPRatio.class);
		conf.setJobName("Top Five Nodes");
		conf.set("mapred.map.tasks","1"); 
	    conf.set("mapred.max.split.size","1000000"); 
			
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
			
		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(Reduce.class);
			
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
			
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
			
		JobClient.runJob(conf);
	}
}
/*
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
*/