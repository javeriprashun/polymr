package com.polymr.osm.poi.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class PoiParserOutTuple implements Writable{

	private String id;
	private String lat;
	private String lon;
    private String tags;
   

   
	public String getTags() {
		return tags;
	}

	public void setTags(String tags) {
		this.tags = tags;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getLat() {
		return lat;
	}

	public void setLat(String lat) {
		this.lat = lat;
	}

	public String getLon() {
		return lon;
	}

	public void setLon(String lon) {
		this.lon = lon;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		
        id=WritableUtils.readString(input);
	    lat=WritableUtils.readString(input);
		lon=WritableUtils.readString(input);
		tags=WritableUtils.readString(input);
		
	}

	@Override
	public void write(DataOutput output) throws IOException {
		// TODO Auto-generated method stub
	       WritableUtils.writeString(output,getId());
	       WritableUtils.writeString(output, getLat());
	       WritableUtils.writeString(output, getLon());
	       WritableUtils.writeString(output, getTags());
	}
	
	@Override
	public String toString(){
		return id+"|"+lat+"|"+lon+"|"+tags;
	}

}
