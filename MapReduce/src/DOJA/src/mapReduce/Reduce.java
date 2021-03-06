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
        List<String> m_List = new ArrayList<String>();
        
        // go through values and load relations "R" and "M"
        // to their corresponding lists. Note that it is ensured
        // by the MapReduce framework that "R" and "M" match on the "key" 
		while (values.hasNext()) {
			String value = values.next().toString();

            if (value.charAt(0) == 'R'){
                r_List.add(value.substring(1));
            }
            else if (value.charAt(0) == 'M'){
                m_List.add(value.substring(1));
            }             
        }
        
        // tuples of relation "M" are generated by inner join
        // of relations "R" and "S". Thus, if there are no
        // tuples of relation "M" then no tuple of relation "R" 
        // could match with relation "S"
        if (m_List.isEmpty()) {
            for (String x : r_List) {
                // emit("key, x, null","") or just count +1
                
                //System.out.println(key + "," + x + "," + null);
                
                //output.collect(new Text(key + "," + x + "," + null), 
                //		NullWritable.get());
                reporter.incrCounter(MyCounter.OUTPUT_TUPLES, 1);
            }
        }
        
        
        // On the other hand, if there is a tuple of relation 
        // "M" then there must be a tuple of relation "R" that
        // matched the relation "S" (so as to generate the tuple
        // of relation "M") 
        else if (m_List.size() > 0) {
            for (String m : m_List) {
                // emit("key, m.x, m.y","") or just count +1
                
                int pos = m.indexOf('_');
                //System.out.println(key + "," + m.substring(0, pos) + 
                //        "," + m.substring(pos + 1));
                
                //output.collect(new Text(key + "," + m.substring(0, pos) + 
                //        "," + m.substring(pos + 1)), 
                //	NullWritable.get());
                reporter.incrCounter(MyCounter.OUTPUT_TUPLES, 1);
            }
        }
        
	}
}