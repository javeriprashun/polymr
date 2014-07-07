package com.polymr.osm.poi.jobs.cleanser;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.polymr.osm.poi.writables.PoiCleanserTuple;

public class PoiCleanserReducer extends Reducer<Text, PoiCleanserTuple, Text, Text> {

private Text outputKey ;

public void reduce(Text key, Iterable<PoiCleanserTuple> values, Context context)
  throws IOException, InterruptedException {
	outputKey = new Text();
		for (PoiCleanserTuple value : values) {
		  outputKey.set(value.toString());
		  context.write(null,outputKey);
		}
}

}