package com.polymr.osm.poi.geocoder;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONObject;

import com.polymr.web.client.ReverseGeocoderClient;

public class PoiGeoCoderMapper extends
Mapper<LongWritable, Text, NullWritable, Text> {
	
	private static final String LATITUDE = "lat";
	private static final String LONGITUDE = "lon";
	private static final String STRING_TOKENIZER_REGEX = "|";

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		try{
	
			String line=value.toString();
			
			int tokenNumber=0;
			StringTokenizer tokenizer = new StringTokenizer(line);
			String latitude= null;
			String longitude=null;
			String id=null;
			while(tokenizer.hasMoreTokens()){
				String token = tokenizer.nextToken(STRING_TOKENIZER_REGEX);
			
				tokenNumber++;
				if(tokenNumber ==1){
					id=token ;
				}
				 if(tokenNumber ==2){
					latitude=token ;
				}
			   if(tokenNumber ==3){
					longitude=token ;
				}
				if(tokenNumber ==4){
			    ReverseGeocoderClient reverseGeocoderClient = new  ReverseGeocoderClient();
				String output=reverseGeocoderClient.reverseGeocode(latitude, longitude);
				  context.write(NullWritable.get(), new Text(output));
				}
			}
			
			/**
		ReverseGeocoderClient reverseGeocoderClient=null;
		String input=value.toString();
		JSONObject json=new JSONObject(input);
		String latitude=String.valueOf(json.get(LATITUDE));
		String longitude=String.valueOf(json.get(LONGITUDE));
	  	 if(latitude != null && !latitude.isEmpty()){
   		  if(longitude != null && !longitude.isEmpty())
   	      reverseGeocoderClient=new ReverseGeocoderClient();
   		  String output=reverseGeocoderClient.reverseGeocode(latitude, longitude);
   	       context.write(NullWritable.get(), new Text(output));
   	      
   	 } */
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
	  }
		
	}
	
