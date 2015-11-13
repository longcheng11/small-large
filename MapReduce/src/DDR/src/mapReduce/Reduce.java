package mapReduce;

import java.io.IOException;
import java.util.Iterator;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

import mapReduce.Main.MyCounter;
 
 
public class Reduce extends Reducer<Text, Text, Text, NullWritable> {

	long maps = 0;
	
    protected void setup(Context context) throws IOException, InterruptedException {
    	Configuration conf = context.getConfiguration();

    	maps = new Long(conf.get("mapred.map.tasks"));    	
    }
    
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        //System.out.println("key: " + key);
        
		//String k = key.toString();
        int non_matched = 0;         

        // non-matched tuples need to be counted. If the
        // number of counted appearances is equal to the 
        // number of map tasks then the non-matched tuple 
        // is emitted
        for (Text value : values) {
        	//String value = values.next().toString();
        	non_matched++;
        }
                		
        if (non_matched == maps){
        	// emit("key.b, key.x, null", "") or just count +1  
        	//System.out.println("" + k.substring(1)); 
        	//context.write(key,NullWritable.get()); 
        	context.getCounter(MyCounter.OUTPUT_TUPLES).increment(1);
        	//reporter.incrCounter(MyCounter.OUTPUT_TUPLES, 1);
        }        
	}
}