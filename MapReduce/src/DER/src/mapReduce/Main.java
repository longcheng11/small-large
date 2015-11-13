package mapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Main {
	
	public static enum MyCounter {
	    OUTPUT_TUPLES
	}; 

	public static void main(String[] args) throws Exception {      
   
        Configuration conf = new Configuration();     

        parseArgsConf(args,conf);
        
        Job job = new Job(conf,"EDBT 2016 DER");
        
        parseArgsJob(args,job);
 
        MultipleOutputs.addNamedOutput(job, "matched", TextOutputFormat.class, Text.class, Text.class);
        
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class); 
 
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
 
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        job.setJarByClass(Main.class);
         
        job.waitForCompletion(true);
        
		Counter c = job.getCounters().findCounter(MyCounter.OUTPUT_TUPLES);
		System.out.println("Number of output tuples: " + c.getValue());
     
 }

	/**
	 * Parse given argument by command line
	 * @param args
	 * @param conf
	 */
	private static void parseArgsConf(String[] args, Configuration conf) {
		
		String[] arguments = args;
	    
	    try {
	    	String arg;
	    	int i = 0;
	    	while (i < arguments.length) {
	    		arg = arguments[i++];
	    		if (arg.equals("-inMemoryInputPath")) {
	    			if (i < arguments.length) {
	    				arg = arguments[i++];
	    				
	    				conf.setStrings("inMemoryInputPath", arg);
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

	/**
	 * Parse given argument by command line
	 * @param args
	 * @param conf
	 */
	private static void parseArgsJob(String[] args, Job job) {
		
		String[] arguments = args;
	    
	    try {
	    	String arg;
	    	int i = 0;
	    	while (i < arguments.length) {
	    		arg = arguments[i++];
	    		if (arg.equals("-inputPath")) {
	    			if (i < arguments.length) {
	    				arg = arguments[i++];
	    				FileInputFormat.setInputPaths(job, new Path(arg));	    	    				
	    			}
	    			else
	    				throw new NumberFormatException();
	    		} 
	    		else if (arg.equals("-outputPath")) {
	    			if (i < arguments.length) {
	    				arg = arguments[i++];
	    				FileOutputFormat.setOutputPath(job, new Path(arg));
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
		            
	    				job.setNumReduceTasks(Integer.valueOf(reduceTasks));	            
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