package org.krisna.multipleMR;

import java.io.IOException;
import java.util.ArrayList;

import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Myreducer1 {

	public static class FirstReducer extends Reducer<Text, Text, Text, IntWritable> {
		public void reduce(Text key,  Iterable<Text> values,Context context1) throws IOException, InterruptedException {

			ArrayList<String> ProductiveNodesList = new ArrayList<String>();
			ProductiveNodesList.add("Standard Error - Tries Exceeded");
			ProductiveNodesList.add("Standard Error");
//			ProductiveNodesList.add("Domestic Deposits - Rates Played");
//			ProductiveNodesList.add("Educational Loans - Rates Played");
//			ProductiveNodesList.add("FCNR Deposits - Rates Played");
//			ProductiveNodesList.add("FOREX Rates Played");
//			ProductiveNodesList.add("Gold Rate Played");
//			ProductiveNodesList.add("Home Loan - Rates Played");
//			ProductiveNodesList.add("Last 5 Transactions on SMS");
//			ProductiveNodesList.add("Mobile Number Registered");
//			ProductiveNodesList.add("Mobile Number De-registered");
//			ProductiveNodesList.add("NRE Rates - Played");
//			ProductiveNodesList.add("OTP Generated");
//			ProductiveNodesList.add("Quick Balance");
//			ProductiveNodesList.add("Details Sent on SMS");

			List<String> UCIDValues = new ArrayList<String>();
			for (Text val : values) {
				UCIDValues.add((val).toString());
		      }
			String ProdrNonProd = "NP";
			for(int i=0;i<ProductiveNodesList.size();i++) {
				if (UCIDValues.contains(ProductiveNodesList.get(i))) {
					ProdrNonProd = "P";
					break;
				}
			}
			context1.write(new Text(ProdrNonProd), new IntWritable(1));
		}
	}

}
