package com.feedutil.utils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.unsafe.types.UTF8String;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feedutil.batch.BatchProcessing;

import scala.Tuple2;

public class DashboardUtils {
	public static String curatedEvents = "feedEvents", curatedEvents_cf = "feedEvents_cf";
	public static String curatedTags = "tags";
	public static String eventTab_cf = "feedEvents_cf";
	public static int server_port = 6000;
	
	//writes the event and tag made via dashboard request into table feedEvents
	@SuppressWarnings({ "deprecation", "unchecked" })
	public static JSONObject addEventsToProcess(String event, String eventTags) {
		JSONObject json = null;
		try {
			json = new JSONObject();
			HTable hTable = new HTable(RSSFeedUtils.fetchHbaseConf(), curatedEvents);
			String rowkey = event.trim().replaceAll(" ", "__");
			Put put = null;
			if(event != null) {
				put = new Put(rowkey.getBytes());
				if(eventTags != null) {
					put.add(Bytes.toBytes(curatedEvents_cf), Bytes.toBytes(curatedTags), Bytes.toBytes(eventTags));
				}
			}
			
			hTable.put(put);
			json.put("status", "event successfully saved");
			hTable.close();
		} catch(IOException e) {
			e.printStackTrace();
		}
		return json;
	}
	
	//method is redundant now....only used in Batch Processing version 1.0
	@SuppressWarnings("deprecation")
	public static Tuple2<ImmutableBytesWritable, Put> processCuratedEvents(String rssFeed, String title, String link, String description, String categories, 
																			String publishedDate) {
		String rowkey = rssFeed + "__" + title;
		Put put = new Put(Bytes.toBytes(rowkey));
		
		put.add(Bytes.toBytes(eventTab_cf), Bytes.toBytes("rssFeed"), Bytes.toBytes(rssFeed));
		put.add(Bytes.toBytes(eventTab_cf), Bytes.toBytes("title"), Bytes.toBytes(title));
		put.add(Bytes.toBytes(eventTab_cf), Bytes.toBytes("link"), Bytes.toBytes(link));
		put.add(Bytes.toBytes(eventTab_cf), Bytes.toBytes("description"), Bytes.toBytes(description));
		put.add(Bytes.toBytes(eventTab_cf), Bytes.toBytes("categories"), Bytes.toBytes(categories));
		put.add(Bytes.toBytes(eventTab_cf), Bytes.toBytes("publishedDate"), Bytes.toBytes(publishedDate));
		
		return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put);
	}
	
	@SuppressWarnings("deprecation")
	public static Tuple2<ImmutableBytesWritable, Put> objectsofCuratedEvents(Object rssFeed, Object title, Object link, Object description, Object categories, 
																				Object articleDate) {
		
		String rowkey = processDateToEpoch(articleDate.toString(), title.toString().length(), categories.toString().split(",").length);
		Put put = new Put(Bytes.toBytes(rowkey));
		
		put.add(Bytes.toBytes(eventTab_cf), Bytes.toBytes("rssFeed"), Bytes.toBytes((String)rssFeed));
		put.add(Bytes.toBytes(eventTab_cf), Bytes.toBytes("title"), Bytes.toBytes((String)title));
		put.add(Bytes.toBytes(eventTab_cf), Bytes.toBytes("link"), Bytes.toBytes((String)link));
		put.add(Bytes.toBytes(eventTab_cf), Bytes.toBytes("description"), Bytes.toBytes(description.toString()));
		put.add(Bytes.toBytes(eventTab_cf), Bytes.toBytes("categories"), Bytes.toBytes((String)categories));
		put.add(Bytes.toBytes(eventTab_cf), Bytes.toBytes("articleDate"), Bytes.toBytes((String)articleDate));
		
		return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put);
	}
	
	private static String processDateToEpoch(String datetime, int titleLength, int categoryLength) {
		SimpleDateFormat df = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
		Date date=null;
		try {
			date = df.parse(datetime);
		} catch(ParseException p) {
			p.printStackTrace();
		}
		
		long result = date.getTime() + titleLength + categoryLength;
		
		return String.valueOf(result);
	}
	
	@SuppressWarnings({ "resource", "deprecation", "unchecked" })
	public static JSONArray fetchEventArticles(String curatedEvent, int numrec) throws IOException {
		Configuration conf = RSSFeedUtils.fetchHbaseConf();
		HBaseAdmin admin = new HBaseAdmin(conf);
		HConnection hConnection = null;
		HTableInterface hTableInterface = null;
		JSONArray jsonArray = new JSONArray();
		JSONObject json = null;
		String event = curatedEvent.trim().replaceAll(" ", "__");
		
		//do a reverse scan on the particular events table to fetch the latest records
		if(admin.tableExists(event)) {
			Scan scan = new Scan();
			scan.setReversed(true);
			scan.setCaching(numrec);
			hConnection = HConnectionManager.createConnection(conf);
			hTableInterface = hConnection.getTable(event);
			ResultScanner scanner = hTableInterface.getScanner(scan);
			int index = 0;
			for(Result r = scanner.next(); r != null; r = scanner.next()) {
				json = new JSONObject();
				
				//fetch all these values
				json.put("rssFeed", Bytes.toString(r.getValue(Bytes.toBytes(eventTab_cf), Bytes.toBytes("rssFeed"))));
				json.put("title", Bytes.toString(r.getValue(Bytes.toBytes(eventTab_cf), Bytes.toBytes("title"))));
				json.put("link", Bytes.toString(r.getValue(Bytes.toBytes(eventTab_cf), Bytes.toBytes("link"))));
				json.put("description", Bytes.toString(r.getValue(Bytes.toBytes(eventTab_cf), Bytes.toBytes("description"))));
				json.put("categories", Bytes.toString(r.getValue(Bytes.toBytes(eventTab_cf), Bytes.toBytes("categories"))));
				json.put("articleDate", Bytes.toString(r.getValue(Bytes.toBytes(eventTab_cf), Bytes.toBytes("articleDate"))));
				
					
				jsonArray.add(json);
				index++;
				//once the required number of records are fetched, exit this scan process
				if(index == numrec || index > numrec) {
					break;
				}
			}
			
			scanner.close();
			hTableInterface.close();
			hConnection.close();
			
		} else {
			json = new JSONObject();
			json.put("value", "store for this curated event hasn't been created!");
			jsonArray.add(json);
		}
		return jsonArray;
	}
	
}
