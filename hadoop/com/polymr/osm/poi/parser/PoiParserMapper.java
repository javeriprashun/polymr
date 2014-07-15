package com.polymr.osm.poi.parser;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.jasper.util.Queue;
import org.w3c.dom.Document;

import com.polymr.osm.poi.util.OSMParserUtil;
import com.polymr.osm.poi.writables.PoiParserOutTuple;

public class PoiParserMapper extends
		Mapper<LongWritable, Text, NullWritable, PoiParserOutTuple> {

	private static final String ENCODING_TYPE="UTF-8";
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		try {
			String document = value.toString();
			DocumentBuilderFactory dbfac = DocumentBuilderFactory.newInstance();
			DocumentBuilder docBuilder = dbfac.newDocumentBuilder();
			InputStream stream = new ByteArrayInputStream(document.getBytes(ENCODING_TYPE));
			Document xmlDocument = docBuilder.parse(stream);
			List<PoiParserOutTuple> poiTupleList = OSMParserUtil.getNodes(xmlDocument);
			for (PoiParserOutTuple Tuple : poiTupleList) {
				context.write(NullWritable.get(), Tuple);
			}
		} catch (Exception e) {
			throw new IOException(e);

		}

	}
}
