package com.polymr.osm.poi.geocoder;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;

public class PoiGeoCoderJobDriver {

    private static final String JOB_NAME = "POI Geocoder";
    private static final String ERROR_PROMPT = "Usage: POI Geocoder  <input path> <output path>";
    
	  public static void main(String[] args) throws Exception {
	    if (args.length != 2) {
	      System.err.println(ERROR_PROMPT);
	      System.exit(-1);
	    }
	    

        Job job = new Job();
	    job.setJarByClass(PoiGeoCoderJobDriver.class);
	    job.setJobName(JOB_NAME);

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setMapperClass(PoiGeoCoderMapper.class);
	
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	       
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
