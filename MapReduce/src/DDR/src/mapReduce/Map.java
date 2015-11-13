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
  
	java.util.Map<Long, java.util.Map<String, Boolean>> inMemoryTuples = null;

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
//    for (Long r_a : inMemoryTuples.keySet()) {
//        for (Long r_x : inMemoryTuples.get(r_a).keySet()) {
//            System.out.println(r_a + "," + r_x + "," + inMemoryTuples.get(r_a).get(r_x));
//        }
//    }

 }
 
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // emit non-matched tuples
        for (Long r_a : inMemoryTuples.keySet()) {
            // if at least one (R.x -> boolean) value is false then all 
            // are, for a matching R.a we go through all R.x during Map
            if (inMemoryTuples.get(r_a).containsValue(Boolean.FALSE)){
                for (String r_x : inMemoryTuples.get(r_a).keySet()) {                
                    //System.out.println(r_a + "," + r_x + "," + null);
        			try {        				
        				context.write(new Text(r_a + "," + r_x + "," + null), 
        						new Text(""));
        			} catch (IOException e) {
        				// TODO Auto-generated catch block
        				e.printStackTrace();
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
            if (inMemoryTuples.containsKey(s_b)){
                for (String r_x : inMemoryTuples.get(s_b).keySet()){
                    inMemoryTuples.get(s_b).put(r_x, Boolean.TRUE);

                    // emit({"1", value.b, x, value.y}, ""); "1" is a flag 
                    // for matched tuples

                    //System.out.println(s_b + "," + r_x + "," + 
                    //        val.substring(pos + 1));
                    //context.write(new Text(s_b + "," + r_x + "," + 
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