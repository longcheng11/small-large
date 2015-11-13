package mapReduce;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import mapReduce.Main.MyCounter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Reduce extends MapReduceBase implements Reducer<LongWritable, Text, Text, NullWritable> {
	
	public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<Text, NullWritable> output, Reporter reporter) throws IOException {
        
        //System.out.println("key: " + key);
        
        List<String> r_List = new ArrayList<String>();
        List<String> s_List = new ArrayList<String>();
        
        // go through values and load relations "R" and "S"  
        // to their corresponding lists.         
		while (values.hasNext()) {
			String value = values.next().toString();

			if (value.charAt(0) == 'R'){
                r_List.add(value.substring(1));
            }
            else if (value.charAt(0) == 'S'){
                s_List.add(value.substring(1));
            }             
        }
        
        // It is ensured by the MapReduce framework that 
        // "R" and "S" match on the "key". Thus, for each
        // tuple of relation "R", we need to generate either
        // a "null" tuple or combine relations "R" and "S".
        for (String x : r_List){
            if (s_List.isEmpty()){
                // emit("key, x, null",""); or just count +1
                
                //System.out.println(key + "," + x + "," + null);
                //output.collect(new Text(key + "," + x + "," + null), 
                //		NullWritable.get());
                reporter.incrCounter(MyCounter.OUTPUT_TUPLES, 1);
            }
            else if (s_List.size() > 0){
                for (String y : s_List){
                    // emit("key, x, y",""); or just count +1
                    
                    //System.out.println(key + "," + x + "," + y);
                    //output.collect(new Text(key + "," + x + "," + y), 
                    //		NullWritable.get());	
                    reporter.incrCounter(MyCounter.OUTPUT_TUPLES, 1);
                }
            }
        }
	}
}