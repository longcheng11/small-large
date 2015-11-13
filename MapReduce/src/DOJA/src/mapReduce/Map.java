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
 	    	
    java.util.Map<Long, java.util.Map<String, Boolean>> inMemoryTuples = null;
	
	
	@Override
	public void configure(JobConf job)  {
    
    	String inMemoryInputPath = null; 
    	String compressed_file_suffix = ".bz2";
        
        // read input path for in memory loaded facts
        if(job.getStrings("inMemoryInputPath") != null &&
    			job.getStrings("inMemoryInputPath")[0] != null){
	    	
        	inMemoryInputPath = job.getStrings("inMemoryInputPath")[0];
        }
        else {
    		System.exit(1);
    	}
        
    	// i.o. initialization
    	BZip2Codec BzipCodec = new BZip2Codec();
    	CompressionInputStream compressedIn = null;
    	DataInputStream plainIn = null;
    	FileSystem fs = null;
		try {
			fs = FileSystem.get(new Configuration());
		} catch (IOException e) {
			e.printStackTrace();
		}
    	BufferedReader reader = null;
    	Path path = null;    	
    	   
    	// check if input file is compressed
    	if(inMemoryInputPath.endsWith(compressed_file_suffix)){
    		path = new Path(inMemoryInputPath);
    		try {
				compressedIn = BzipCodec.createInputStream(fs.open(path));
			} catch (IOException e) {
				e.printStackTrace();
			}
    		reader = new BufferedReader(new InputStreamReader(compressedIn));
    	}
    	else {
    		try {
				fs = FileSystem.get(new Configuration());
				plainIn = new DataInputStream(fs.open(new Path(inMemoryInputPath)));
			} catch (IOException e) {
				e.printStackTrace();
			}
        	reader = new BufferedReader(new InputStreamReader(plainIn));
    	}
    	
    	// initialize strings
    	String str = null,line = null;

    	Boolean init_tag = null;
        
        Long r_a = null;
        String r_x = null;
        
        inMemoryTuples = new HashMap<Long, java.util.Map<String, Boolean>>();
        
        try {
			while ((line = reader.readLine()) != null){
				str = new String(line).trim();
		
	            // extract R.a -> R.x 
	            int r_pos = str.indexOf("|");
	            r_a = new Long(str.substring(0,r_pos));
	            r_x = str.substring(r_pos + 1);

	            // if R.a is not in memory
	            if(!inMemoryTuples.containsKey(r_a)){
	                java.util.Map<String, Boolean> tmpMap = new java.util.HashMap<String, Boolean>();
	                tmpMap.put(r_x, init_tag);
	                inMemoryTuples.put(r_a, tmpMap);
	            } else {
	                // if R.a is in memory but R.x is not
	                if(!inMemoryTuples.get(r_a).containsKey(r_x)){
	                        inMemoryTuples.get(r_a).put(r_x, init_tag);
	                }
	            }           		    			        
           		    			
			}
			
			reader.close();
			
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
    	
        // print inMemoryTuples
//        for (Long r_a : inMemoryTuples.keySet()) {
//            for (Long r_x : inMemoryTuples.get(r_a).keySet()) {
//                System.out.println(r_a + "," + r_x + "," + inMemoryTuples.get(r_a).get(r_x));
//            }
//        }
    }
 	
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
        
        // this is a pass through operation for relation
        // "R" required by step 4 of DOJA algorithm
        //if (r_pos >= 0){
 		if (c_at == '|'){
            //System.out.println(new Long(val.substring(0, pos)) + "," +
            //        "R" + val.substring(pos + 1));
            output.collect(new LongWritable(new Long(val.substring(0, pos))), 
              new Text("R" + val.substring(pos + 1)));	

        // This is step 2 of DOJA algorithm
        //}else if (s_pos >= 0) {
 		} else if (c_at == '*') {
            Long s_b = new Long(val.substring(0, pos));
            if (inMemoryTuples.containsKey(s_b)){
                for (String r_x : inMemoryTuples.get(s_b).keySet()){
                    // emit(value.b, {"M", x, value.y});

                    //System.out.println(s_b + ",M" + r_x + "_" + 
                    //        val.substring(pos + 1));
                    output.collect(new LongWritable(s_b), 
                      new Text("M" + r_x + "_" + val.substring(pos + 1)));	
                }
            }
        }
   }
}