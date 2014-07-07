package com.polymr.osm.poi.jobs.binning;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONException;
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
			StringTokenizer tokenizer = new StringTokenizer(line);
			OSMSorterUtil.getRestaurants(tokenizerToJsonString(tokenizer), mos);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private JSONObject tokenizerToJsonString(StringTokenizer tokenizer) {

		int tokenNumber = 0;
		List<String> tokenList = new ArrayList<String>();
		JSONObject json = null;

		try {
			while (tokenizer.hasMoreTokens()) {
				String token = tokenizer.nextToken(STRING_TOKENIZER_REGEX);
				tokenNumber++;
				if (tokenNumber == 4) {
					json = new JSONObject(token);
					json.put(PoiConstants.POINT_OF_INTEREST_ID,
							tokenList.get(0));
					json.put(PoiConstants.POINT_OF_INTEREST_LATITUDE,
							tokenList.get(1));
					json.put(PoiConstants.POINT_OF_INTEREST_LONGITUDE,
							tokenList.get(2));
				} else if (tokenNumber == 1) {
					tokenList.add(0, token);
				} else if (tokenNumber == 2) {
					tokenList.add(1, token);
				} else if (tokenNumber == 3) {
					tokenList.add(2, token);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return json;
	}

	public void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}
}
