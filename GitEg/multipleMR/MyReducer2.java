package org.krisna.multipleMR;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer2 {

	public static class SecondReducer  extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key,  Iterable<Text> values,Context context) throws IOException, InterruptedException {

			List<String> PorNPlist = new ArrayList<String>();
			long TotalCnt = 0;
			for (Text val : values) {
				PorNPlist.add((val).toString());
				TotalCnt++;
		      }
			context.write(new Text("Total number of call received"), new Text(TotalCnt+""));
			Map<String, Integer> PorNPMap = new HashMap<String, Integer>();
			for (String temp : PorNPlist) {
					Integer count = PorNPMap.get(temp);
					PorNPMap.put(temp, (count == null) ? 1 : count + 1);
			}
			
			for (Map.Entry<String, Integer> entry : PorNPMap.entrySet()) {
				if(entry.getKey().equals("P")) {
					
					double ProdPercent = (((double) entry.getValue())/(double) TotalCnt)*100;
					context.write(new Text("Total number Of Productive calls"), new Text(entry.getValue()+" ("+ProdPercent+"%)"));

				}
				else if(entry.getKey().equals("NP")) {
					double ProdPercent = ((double) entry.getValue()/(double) TotalCnt)*100;
					context.write(new Text("Total number Of Non-Productive calls"), new Text(entry.getValue()+" ("+ProdPercent+"%)"));
				}
			}
		}
	}

}
