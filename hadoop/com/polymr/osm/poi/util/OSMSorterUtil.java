package com.polymr.osm.poi.util;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.codehaus.jettison.json.JSONObject;

import com.polymr.osm.poi.common.PoiConstants;

public class OSMSorterUtil {

	public static void getRestaurants( JSONObject json,MultipleOutputs< Text, NullWritable> mos){
		 String str=json.toString();

		 try{

		 if( json.has(PoiConstants.AMENITIES) ){
			 if(json.get(PoiConstants.AMENITIES).equals(PoiConstants.RESTURANT))
			 {
			 mos.write(PoiConstants.RESTURANT,new Text(str), NullWritable.get());
			 }
			 if(json.get(PoiConstants.AMENITIES).equals(PoiConstants.FAST_FOOD_RESTURANT))
			 {
			 mos.write(PoiConstants.RESTURANT,new Text(str), NullWritable.get());
			 }
			 if(json.get(PoiConstants.AMENITIES).equals(PoiConstants.CAFE))
			 {
			 mos.write(PoiConstants.RESTURANT,new Text(str), NullWritable.get());
			 }
		 }
	  }
		 catch(Exception e){
			 e.printStackTrace();
		 }
	}
}
