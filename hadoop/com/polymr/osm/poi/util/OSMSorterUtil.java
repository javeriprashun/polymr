package com.polymr.osm.poi.util;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.codehaus.jettison.json.JSONObject;

import com.polymr.osm.poi.common.PoiConstants;

public class OSMSorterUtil {

	public static void getRestaurants(JSONObject json,
			MultipleOutputs<Text, NullWritable> mos) {
		String str = json.toString();

		try {

			if (json.has(PoiConstants.RESTURANT)) {
					mos.write(PoiConstants.RESTURANT, new Text(str),NullWritable.get());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void getBanks(JSONObject json,MultipleOutputs<Text, NullWritable> mos) {
		String str = json.toString();

		try {
			if (json.has(PoiConstants.BANK)) {
					mos.write(PoiConstants.BANK, new Text(str),NullWritable.get());
			}
		} catch (Exception e) {
			e.getStackTrace();
		}
	}
	public static void getGrocery(JSONObject json,MultipleOutputs<Text, NullWritable> mos) {
		String str = json.toString();

		try {
			if (json.has(PoiConstants.GROCERY)|json.has(PoiConstants.GREEN_GROCER)) {
				mos.write(PoiConstants.GROCERY, new Text(str),NullWritable.get());
			}
		} catch (Exception e) {
			e.getStackTrace();
		}
	}
	public static void getDepartmentStore(JSONObject json,MultipleOutputs<Text, NullWritable> mos) {
		String str = json.toString();

		try {
			if (json.has(PoiConstants.DEPARTMENT_STORE )) {
				mos.write(PoiConstants.DEPARTMENTSTORE , new Text(str),NullWritable.get());
			}
		} catch (Exception e) {
			e.getStackTrace();
		}
	}
	
	//department_store
}
