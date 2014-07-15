package com.polymr.osm.poi.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jettison.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.polymr.osm.poi.writables.PoiParserOutTuple;

public class OSMParserUtil {

	private static final String KEY = "k";
	private static final String VALUE = "v";
	private static final String NODE_ID = "id";
	private static final String LATITUDE = "lat";
	private static final String LONGITUDE = "lon";
	private static final String NODE = "node";
	private static final String EMPTY_BRACKETS = "{}";

	@SuppressWarnings("nls")
	public static List<PoiParserOutTuple> getNodes(Document xmlDocument) {

		List<PoiParserOutTuple> pointOfInterestTupleList = new ArrayList<PoiParserOutTuple>();

		Node osmRoot = xmlDocument.getFirstChild();
		NodeList osmXMLNodes = osmRoot.getChildNodes();
		for (int i = 1; i < osmXMLNodes.getLength(); i++) {
			Node item = osmXMLNodes.item(i);
			if (item.getNodeName().equals(NODE)) {
				PoiParserOutTuple pointOfInterestTuple = new PoiParserOutTuple();
				NamedNodeMap attributes = item.getAttributes();
				NodeList tagXMLNodes = item.getChildNodes();
				Map<String, String> tags = new HashMap<String, String>();
				JSONObject tagsJson = null;
				for (int j = 1; j < tagXMLNodes.getLength(); j++) {
					Node tagItem = tagXMLNodes.item(j);
					NamedNodeMap tagAttributes = tagItem.getAttributes();
					tagsJson = new JSONObject(tags);
					if (tagAttributes != null) {
						String value = tagAttributes.getNamedItem(VALUE).getNodeValue();
						String key = tagAttributes.getNamedItem(KEY).getNodeValue();
						Node namedItemID = attributes.getNamedItem(NODE_ID);
						Node namedItemLat = attributes.getNamedItem(LATITUDE);
						Node namedItemLon = attributes.getNamedItem(LONGITUDE);
						tags.put(key, value);
						String id = namedItemID.getNodeValue();
						pointOfInterestTuple.setId(id);
						String latitude = namedItemLat.getNodeValue();
						pointOfInterestTuple.setLat(latitude);
						String longitude = namedItemLon.getNodeValue();
						pointOfInterestTuple.setLon(longitude);
						pointOfInterestTuple.setTags(tagsJson.toString());
						if (!tagsJson.toString().equals(EMPTY_BRACKETS)) {
							if (!pointOfInterestTupleList.contains(pointOfInterestTuple)) {
								 pointOfInterestTupleList.add(pointOfInterestTuple);
							}
						}

					}
				}
			}
		}

		return pointOfInterestTupleList;
	}

}