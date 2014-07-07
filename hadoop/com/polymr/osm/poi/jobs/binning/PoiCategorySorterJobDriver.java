package com.polymr.osm.poi.jobs.binning;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.NullWritable;

import com.polymr.osm.poi.common.PoiConstants;

public class PoiCategorySorterJobDriver {

    private static final String JOB_NAME = "POI Category Sorter";
    private static final String ERROR_PROMPT = "Usage: POI Category Sorter <input path> <output path>";
    
	  public static void main(String[] args) throws Exception {
	    if (args.length != 2) {
	      System.err.println(ERROR_PROMPT);
	      System.exit(-1);
	    }
	    
	   
        Job job = new Job();
	    job.setJarByClass(PoiCategorySorterJobDriver.class);
	    job.setJobName(JOB_NAME);

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setMapperClass(PoiCategorySorterMapper.class);
	
	    job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
       
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(NullWritable.class);

	    MultipleOutputs.addNamedOutput(job,PoiConstants.RESTURANT,TextOutputFormat.class, Text.class, NullWritable.class);
	    
        MultipleOutputs.setCountersEnabled(job, true);
        
        job.setNumReduceTasks(0);
        
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
