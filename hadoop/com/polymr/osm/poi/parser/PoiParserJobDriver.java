package com.polymr.osm.poi.parser;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;

import com.polymr.osm.poi.inputs.PoiParserrInput;
import com.polymr.osm.poi.writables.PoiParserOutTuple;

public class PoiParserJobDriver {

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
	    job.setJarByClass(PoiParserJobDriver.class);
	    job.setJobName(JOB_NAME);

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setMapperClass(PoiParserMapper.class);
	   // job.setReducerClass(PoiCleanserReducer.class);

	    job.setInputFormatClass(PoiParserrInput.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(PoiParserOutTuple.class);
	       
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
