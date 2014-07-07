package com.polymr.osm.poi.jobs.cleanser;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;

import com.polymr.osm.poi.inputs.PoiCleanserInput;
import com.polymr.osm.poi.writables.PoiCleanserTuple;

public class PoiCleanserJobDriver {

	private static final String START_TAG_KEY = "xmlinput.start";
    private static final String END_TAG_KEY = "xmlinput.end";
	private static final String START_TAG_VALUE = "<osm";
    private static final String END_TAG_VALUE = "osm>";
    private static final String JOB_NAME = "PointsOfInterestCleanser";
    private static final String ERROR_PROMPT = "Usage: Points Of Interest Cleanser  <input path> <output path>";
    
	  public static void main(String[] args) throws Exception {
	    if (args.length != 2) {
	      System.err.println(ERROR_PROMPT);
	      System.exit(-1);
	    }
	    
	    Configuration conf = new Configuration();

        conf.set(START_TAG_KEY, START_TAG_VALUE );
        conf.set(END_TAG_KEY, END_TAG_VALUE);
        Job job = new Job(conf);
	    job.setJarByClass(PoiCleanserJobDriver.class);
	    job.setJobName(JOB_NAME);

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setMapperClass(PoiCleanserMapper.class);
	    job.setReducerClass(PoiCleanserReducer.class);

	    job.setInputFormatClass(PoiCleanserInput.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(PoiCleanserTuple.class);
	       
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
