package com.polymr.osm.poi.sorter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONObject;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.polymr.osm.poi.common.PoiConstants;
import com.polymr.osm.poi.util.OSMSorterUtil;

public class PoiCategorySorterMapper extends
		Mapper<LongWritable, Text, Text, NullWritable> {

	private static final String STRING_TOKENIZER_REGEX = "|";

	private MultipleOutputs<Text, NullWritable> mos = null;

	protected void setup(Context context) {
		mos = new MultipleOutputs<Text, NullWritable>(context);
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		try {
			String line = value.toString();

			JSONObject json=new JSONObject(line);
			JSONObject address=(JSONObject)json.get("address");
				
			//Food, beverages
			
			
			OSMSorterUtil.getRestaurants(address, mos);
			OSMSorterUtil.getBanks(address,mos);
			OSMSorterUtil.getGrocery(address,mos);
			OSMSorterUtil.getDepartmentStore(address,mos);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}
}