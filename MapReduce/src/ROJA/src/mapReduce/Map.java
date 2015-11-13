package mapReduce;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.NullWritable;

public class Map extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
 	    	 	
 	public void map(LongWritable key, Text value, 
 			OutputCollector<LongWritable, Text> output, Reporter reporter) 
 				throws IOException {

 		String val = value.toString();
 		
 		int pos = (val.charAt(0) == '-')? 0 : -1, c_at = -1;
 		
 		do {
 			pos++;
 			c_at = val.charAt(pos); 			
 		} while (c_at >= '0' && c_at <= '9');
 		
        //int r_pos = val.indexOf('|');
        //int s_pos = val.indexOf('*');
        
        //if (r_pos >= 0){
        if (c_at == '|'){
            //System.out.println(new Long(val.substring(0, pos)) + "," +
            //        "R" + val.substring(pos + 1));
            output.collect(new LongWritable(new Long(val.substring(0, pos))), 
              new Text("R" + val.substring(pos + 1)));	
        //} else if (s_pos >= 0) {
        } else if (c_at == '*') {
            //System.out.println(new Long(val.substring(0, pos)) + "," +
            //        "S" + val.substring(pos + 1));
            output.collect(new LongWritable(new Long(val.substring(0, pos))), 
              new Text("S" + val.substring(pos + 1)));	
        }
  }
}