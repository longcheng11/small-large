package mapReduce;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.StringTokenizer;
 
import mapReduce.Main.MyCounter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Map extends Mapper<Object, Text, Text, Text> {
  
	java.util.Map<Long, java.util.Map<String, java.util.Map<Long, Boolean>>> inMemoryTuplesWithID = null;

	private MultipleOutputs<Text, Text> mos;
	
    protected void setup(Context context) throws IOException, InterruptedException {
   	 
    	mos = new MultipleOutputs<Text, Text>(context);
    	
    	Configuration conf = context.getConfiguration();

    	String inMemoryInputPath = null; 
    	String compressed_file_suffix = ".bz2";
    
    	// read input path for in memory loaded facts
    	if(conf.getStrings("inMemoryInputPath") != null &&
			conf.getStrings("inMemoryInputPath")[0] != null){
    	
    		inMemoryInputPath = conf.getStrings("inMemoryInputPath")[0];
    	}
    	else {
    		System.out.println("inMemoryInputPath is null! : " + 
    				(conf.getStrings("inMemoryInputPath") != null));
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

    	Boolean init_tag = false;
        
        Long r_a = null, r_id = new Long(0);
        String r_x = null;
        int r_pos;
        
        inMemoryTuplesWithID = new HashMap<Long, java.util.Map<String, java.util.Map<Long, Boolean>>>();
        
        try {
			while ((line = reader.readLine()) != null){
				str = new String(line).trim();
		
	            // extract R.a -> R.x 
	            r_pos = str.indexOf("|");
	            r_a = new Long(str.substring(0,r_pos));
	            r_x = str.substring(r_pos + 1);

	            // if R.x is not in memory
	            if(!inMemoryTuplesWithID.containsKey(r_a)){
	                java.util.Map<Long, Boolean> tmpMap = new java.util.HashMap<Long, Boolean>();
	                java.util.Map<String, java.util.Map<Long, Boolean>> tmpMap2 = new java.util.HashMap<String, java.util.Map<Long, Boolean>>();

	                tmpMap.put(r_id, init_tag);
	                tmpMap2.put(r_x, tmpMap);

	                inMemoryTuplesWithID.put(r_a, tmpMap2);
	            } else {
	                // if R.x is in memory but R.x is not
	                if(!inMemoryTuplesWithID.get(r_a).containsKey(r_x)){
	                    java.util.Map<Long, Boolean> tmpMap = new java.util.HashMap<Long, Boolean>();				    	
	                    tmpMap.put(r_id, init_tag);

	                    inMemoryTuplesWithID.get(r_a).put(r_x, tmpMap);
	                }
	                else {
	                    if(!inMemoryTuplesWithID.get(r_a).get(r_x).containsKey(r_id)){
	                            inMemoryTuplesWithID.get(r_a).get(r_x).put(r_id, init_tag);
	                    }
	                }
	            } 
	            
	            r_id++;
			}
			
			reader.close();
			
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
    	
        // print inMemoryIDToTuples
//        for (Long r_id : inMemoryIDToTuples.keySet()) {
//            System.out.println(r_id + "," + inMemoryIDToTuples.get(r_id).get(0) + 
//                    "," + inMemoryIDToTuples.get(r_id).get(1));
//        }

 }
 
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // emit non-matched tuples
        for (Long r_a : inMemoryTuplesWithID.keySet()) {
            for (String r_x : inMemoryTuplesWithID.get(r_a).keySet()) {
                // if at least one (id -> boolean) value is
                // false then all are, for a matching R.a
                // we go through all R.x -> R.id during Map
                if (inMemoryTuplesWithID.get(r_a).get(r_x).containsValue(Boolean.FALSE)){
                    for (Long r_id : inMemoryTuplesWithID.get(r_a).get(r_x).keySet()) {                
                        //System.out.println(r_id.toString());
                        
                    	try {
                    		context.write(new Text(r_id.toString()), 
            						new Text(""));
            			} catch (IOException e) {
            				// TODO Auto-generated catch block
            				e.printStackTrace();
            			}
                    }
                }
            }
        }        

        mos.close();
    }
   
    @Override
    public void map(Object key, Text value,
    		Context context) throws IOException, InterruptedException {
  
		String val = value.toString();
 		
        //System.out.println("value: " + val);
        
		int pos = (val.charAt(0) == '-')? 0 : -1, c_at = -1;
 		
 		do {
 			pos++;
 			c_at = val.charAt(pos); 			
 		} while (c_at >= '0' && c_at <= '9');

        //int s_pos = val.indexOf('*'); 
        
        // This is step 2 of DDR algorithm
        // emit matched tuples        
        //if (s_pos >= 0) {
 		if (c_at == '*'){
            Long s_b = new Long(val.substring(0, pos));
            if (inMemoryTuplesWithID.containsKey(s_b)){
                for (String r_x : inMemoryTuplesWithID.get(s_b).keySet()){
                    for (Long r_id : inMemoryTuplesWithID.get(s_b).get(r_x).keySet()) {
                        inMemoryTuplesWithID.get(s_b).get(r_x).put(r_id, Boolean.TRUE);
                    }
                    
                    // emit({"1", value.b, x, value.y}, ""); // "1" is a flag 
                    // for matched tuples

                    //System.out.println(s_b + "," + r_x + "," + 
                    //        val.substring(s_pos + 1));
                    //context.write(new Text("s_b + "," + r_x + "," + 
                    //        val.substring(pos + 1)), new Text(""));
                    context.getCounter(MyCounter.OUTPUT_TUPLES).increment(1);
                    //mos.write("matched", 
                    //		new Text(s_b + "," + r_x + "," + 
                    //                val.substring(pos + 1)), 
                    //                new Text(""));

                }
            }
        }
 	
    }
}