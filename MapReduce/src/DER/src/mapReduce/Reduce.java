package mapReduce;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

import mapReduce.Main.MyCounter;
 
 
public class Reduce extends Reducer<Text, Text, Text, NullWritable> {

	private class R_holder {
		Long a;
		String x;		
		
		R_holder(Long a, String x){
			this.a = a;
			this.x = x;
		}

		public Long getA() {
			return a;
		}

		public String getX() {
			return x;
		}		
		
	}
	
	java.util.Map<Long, R_holder> inMemoryIDToTuples = null;
	long maps = 0;
	
    protected void setup(Context context) throws IOException, InterruptedException {
    	Configuration conf = context.getConfiguration();

    	maps = new Long(conf.get("mapred.map.tasks"));

		
    	String inMemoryInputPath = null; 
    	String compressed_file_suffix = ".bz2";
        
        // read input path for in memory loaded facts
        if(conf.getStrings("inMemoryInputPath") != null &&
        		conf.getStrings("inMemoryInputPath")[0] != null){
	    	
        	inMemoryInputPath = conf.getStrings("inMemoryInputPath")[0];
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
        
        Long r_a = null, r_id = new Long(0);
        String r_x = null;
        int r_pos;
                
        inMemoryIDToTuples = new HashMap<Long, R_holder>();
        
        try {
			while ((line = reader.readLine()) != null){
				str = new String(line).trim();

	            // extract R.a -> R.x 
	            r_pos = str.indexOf("|");
	            r_a = new Long(str.substring(0,r_pos));
	            r_x = str.substring(r_pos + 1);
	            
	            // if R.id is not in memory
	            if(!inMemoryIDToTuples.containsKey(r_id)){
	                inMemoryIDToTuples.put(r_id, new R_holder(r_a, r_x));
	            } else {
	                System.out.println("Error: Tuple id must be unique!");
	                System.exit(0);
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
  //      for (Long r_id : inMemoryIDToTuples.keySet()) {
  //          System.out.println(r_id + "," + inMemoryIDToTuples.get(r_id).get(0) + 
  //                  "," + inMemoryIDToTuples.get(r_id).get(1));
  //      }

    }
    
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        //System.out.println("key: " + key);
        
		//String k = key.toString();
        int ids = 0;

        // non-matched tuples need to be counted. If the
        // number of counted appearances is equal to the 
        // number of map tasks then the non-matched tuple 
        // is emitted
        for (Text value : values) {
        	//String value = values.next().toString();
        	ids++;
        }
            
        if (ids == maps){
        	// emit("key.b, key.x, null", "") or just count +1
        	Long r_id = new Long(key.toString());
        	//System.out.println(inMemoryIDToTuples.get(r_id).getA() + "," + 
        	//        inMemoryIDToTuples.get(r_id).getX() + "," + null);
        	//context.write(new Text(inMemoryIDToTuples.get(r_id).getA() + "," + 
        	//		inMemoryIDToTuples.get(r_id).getX() + "," + null),
        	//		NullWritable.get()); 
        	context.getCounter(MyCounter.OUTPUT_TUPLES).increment(1);                       
        }		
	}
}