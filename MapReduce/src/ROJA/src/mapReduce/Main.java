package mapReduce;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
 	
public class Main {
 	   
	protected static enum MyCounter {
	    OUTPUT_TUPLES
	};
	
	public static void main(String[] args) throws Exception {
		
		JobConf conf = new JobConf(Main.class);
		parseArgs(args,conf);
		
		conf.setJobName("EDBT 2016 ROJA");
 	    
		conf.setOutputKeyClass(Text.class);
 	    conf.setOutputValueClass(Text.class);
	 
		conf.setMapperClass(Map.class);
 	    //conf.setCombinerClass(Combine.class);
	    conf.setReducerClass(Reduce.class);
		
		conf.setMapOutputKeyClass(LongWritable.class);
		conf.setMapOutputValueClass(Text.class);
		
		conf.setInputFormat(TextInputFormat.class);
 		conf.setOutputFormat(TextOutputFormat.class); 	 	
 		
// 		conf.setNumMapTasks(1);
// 		conf.setNumReduceTasks(0);
 		
//		FileInputFormat.setInputPaths(conf, new Path("/Stratified/Input/DerivedData"),new Path("/Stratified/Input/Reasoning/Facts"));
//		FileOutputFormat.setOutputPath(conf, new Path("/Stratified/Input/Reasoning/FiredRules"));
		
 		RunningJob job = JobClient.runJob(conf);  
		Counters c = job.getCounters();
		long cnt = c.getCounter(MyCounter.OUTPUT_TUPLES);
		System.out.println("Number of output tuples: " + cnt);
	} 	
	
	/**
	 * Parse given argument by command line
	 * @param args
	 * @param conf
	 */
	private static void parseArgs(String[] args, JobConf conf) {
		
		String[] arguments = args;
	    
	    try {
	    	String arg;
	    	int i = 0;
	    	while (i < arguments.length) {
	    		arg = arguments[i++];
	    		if (arg.equals("-inputPath")) {
	    			if (i < arguments.length) {
	    				arg = arguments[i++];
	    				FileInputFormat.setInputPaths(conf, new Path(arg));	    	    				
	    			}
	    			else
	    				throw new NumberFormatException();
	    		} 
	    		else if (arg.equals("-outputPath")) {
	    			if (i < arguments.length) {
	    				arg = arguments[i++];
	    				FileOutputFormat.setOutputPath(conf, new Path(arg));
	    			}
	    			else
	    				throw new NumberFormatException();
	    		} 
	    		else if (arg.equals("-inMemoryInputPath")) {
	    			if (i < arguments.length) {
	    				arg = arguments[i++];
	    				
	    				conf.setStrings("inMemoryInputPath", arg);
	    			} 
	    			else
	    				throw new NumberFormatException();
	    		}
	    		else if (arg.equals("-mapTasks")) {
	    			if (i < arguments.length) {
	    				arg = arguments[i++];
	    				
	    				int mapTasks = Integer.parseInt(arg);
	    				if (mapTasks < 1)
	    					mapTasks = 1;
		            
	    				conf.setNumMapTasks(Integer.valueOf(mapTasks));	            
	    			}
	    			else
	    				throw new NumberFormatException();
		        } 
	    		else if (arg.equals("-reduceTasks")) {
	    			if (i < arguments.length) {
	    				arg = arguments[i++];
	    				
	    				int reduceTasks = Integer.parseInt(arg);
	    				if (reduceTasks < 0)
	    					reduceTasks = 1;
		            
	    				conf.setNumReduceTasks(Integer.valueOf(reduceTasks));	            
	    			}
	    			else
	    				throw new NumberFormatException();
		        } 
	    		else if (arg.equals("-compressMapOutput")) {
	    			if (i < arguments.length) {
	    				arg = arguments[i++];
	    				
	    				if (arg.equals("true")){
	    					conf.set("mapred.compress.map.output", "true");
	    					conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
	    				}
	    			}
	    			else
	    				throw new NumberFormatException();
	    		}
	    		else if (arg.equals("-compressOutput")) {
	    			if (i < arguments.length) {
	    				arg = arguments[i++];
	    				
	    				if (arg.equals("true")){
	    					conf.set("mapred.output.compress", "true");
	    					conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");	    					
	    				}
	    			}
	    			else
	    				throw new NumberFormatException();
	    		}
	    	}
	    }
	    catch (Exception e) {   
	    	System.err.println("Usage: Single Argument Reasoner\n" +
	    		"\t[-inputPath <input path>]\n" +        
	    		"\t[-outputPath <output path>]\n" +
	    		"\t[-inMemoryInputPath <input path for in memory loaded facts>]\n" +	    		
	    		"\t[-mapTasks <number of map tasks>]\n" +              
	    		"\t[-reduceTasks <number of reduce tasks>]\n" +              
	    		"\t[-combine <true/false: enable combine>]\n" +
	    		"\t[-compressMapOutput <true/false:compress intermediate pairs (map tasks output)>]\n" +
	    		"\t[-compressOutput <true/false:compress job's output (mapreduce output)>]\n"
	    );
	    // usage example: -inputPath /facts/uniform -outputPath /test/output -inMemoryInputPath /facts/single_multi_in_memory/zipf/file.bz2 -mapTasks 5 -reduceTasks 0 -compressMapOutput true -compressOutput true -ruleset teams_1 
	    System.exit(0);
	    }
	}

}