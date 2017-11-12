package com.feedutil.utils;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import com.aylien.textapi.TextAPIClient;
import com.aylien.textapi.TextAPIException;
import com.aylien.textapi.parameters.CombinedParams;
import com.aylien.textapi.parameters.HashTagsParams;
import com.aylien.textapi.responses.Combined;
import com.aylien.textapi.responses.HashTags;
import com.feedutil.rssfeed.FeedObject;
import com.feedutil.schema.FeedData;
import com.feedutil.schema.SerializableFeedData;

import scala.Tuple2;

public class RSSFeedUtils {
	public static String kafkaTopic = "rssFeed1";
	public static String kafkaGroupID = "rssFeedGroupID";
	public static String kafkaConsumerID = "rssFeedConsumerID";
	public static String server_IP = "xxx.xx.x.xx", 
						kafkaPort = "9092", 
						zookeeperPort = "2181", 
						zkTimeout = "30000";
	public static String alphatable = "alphaFeed", 
						betatable = "betaFeed", 
						hbaseTab_cf = "feed_cf", 
						hbasePort = "60000";
	public static int numThreads = 1;
	private static String appID = "*id*", apiKey = "**key**";
	
	//to set up kafka properties
	public static Properties fetchKafkaProperties() {
		Properties props = new Properties();
		props.put("metadata.broker.list", server_IP + ":" + kafkaPort);
		props.put("serializer.class", "kafka.serializer.DefaultEncoder");
		props.put("kafka.consumer.id", kafkaConsumerID);
		props.put("group.id", kafkaGroupID);
		props.put("zookeeper.connect", server_IP + ":" + zookeeperPort);
		props.put("zookeeper.connection.timeout.ms", zkTimeout);
		return props;
	}
	
	//to set up apache spark properties
	public Map<String, String> fetchSparkProperties() {
		Map<String, String> sparkUtils = new HashMap<String, String>();
		sparkUtils.put("appname", "RSSFeedProcessing");
		sparkUtils.put("cores", "local[3]");
		sparkUtils.put("spark_serializer", "org.apache.spark.serializer.JavaSerializer");
		sparkUtils.put("driverMemory", "2g");
		sparkUtils.put("batchSize", "2000");
		sparkUtils.put("blockSize", "1000");
		sparkUtils.put("scheduler", "FAIR");
		sparkUtils.put("spark_checkpoint_dir", "/home/spark/sparkChkPoint");
		
		return sparkUtils;
	}
	
	public static Configuration fetchHbaseConf() {
		Configuration hbaseconf = HBaseConfiguration.create();
		hbaseconf.set("hbase.master", server_IP + ":" + hbasePort);
		hbaseconf.set("hbase.zookeeper.quorum", server_IP);
		hbaseconf.set("hbase.zookeeper.property.clientPort", zookeeperPort);
		
		return hbaseconf;
	}
	
	private static TextAPIClient client = new TextAPIClient(appID, apiKey);
	
	//dumps data to alphaFeed
	@SuppressWarnings("deprecation")
	public static void dumpFeedIntoAlpha(FeedData sFeedData) {
		try {
			HTable hbaseAlpha = new HTable(fetchHbaseConf(), RSSFeedUtils.alphatable);
			String rowkey = (sFeedData.getTitle() + "__" + sFeedData.getDescription());
			Put put = new Put(rowkey.getBytes());
			
			if(sFeedData.getRSSFeed() != null) {
				put.add(Bytes.toBytes(hbaseTab_cf), Bytes.toBytes("rssFeed"), Bytes.toBytes(sFeedData.getRSSFeed()));
			}
			if(sFeedData.getTitle() != null) {
				put.add(Bytes.toBytes(hbaseTab_cf), Bytes.toBytes("title"), Bytes.toBytes(sFeedData.getTitle()));
			}
			if(sFeedData.getLanguage() != null) {
				put.add(Bytes.toBytes(hbaseTab_cf), Bytes.toBytes("language"), Bytes.toBytes(sFeedData.getLanguage()));
			}
			if(sFeedData.getThumbnail() != null) {
				put.add(Bytes.toBytes(hbaseTab_cf), Bytes.toBytes("thumbnail"), Bytes.toBytes(sFeedData.getThumbnail()));
			}
			if(sFeedData.getArticleLink() != null) {
				put.add(Bytes.toBytes(hbaseTab_cf), Bytes.toBytes("articleLink"), Bytes.toBytes(sFeedData.getArticleLink()));
			}
			if(sFeedData.getDescription() != null) {
				put.add(Bytes.toBytes(hbaseTab_cf), Bytes.toBytes("description"), Bytes.toBytes(sFeedData.getDescription()));
			}
			if(sFeedData.getAuthor() != null) {
				put.add(Bytes.toBytes(hbaseTab_cf), Bytes.toBytes("author"), Bytes.toBytes(sFeedData.getAuthor()));
			}
			if(sFeedData.getPublishedDate() != null) {
				put.add(Bytes.toBytes(hbaseTab_cf), Bytes.toBytes("publishedDate"), Bytes.toBytes(sFeedData.getPublishedDate()));
				
				long ts = sFeedData.getPublishedDate();
				//get the article date and time from the epch timestamp in published date
				Date date = new Date(ts);
				String datestring = date.getDate()+"-"+(date.getMonth()+1) + "-" + (date.getYear()+1900) + " " + date.getHours() + 
									":" + date.getMinutes() + ":" + date.getSeconds();
				
				put.add(Bytes.toBytes(hbaseTab_cf), Bytes.toBytes("articleDate"), Bytes.toBytes(datestring));
				
			}
			if(sFeedData.getUpdatedDate() != null) {
				put.add(Bytes.toBytes(hbaseTab_cf), Bytes.toBytes("updatedDate"), Bytes.toBytes(sFeedData.getUpdatedDate()));
			}
			if(sFeedData.getLinkHref() != null) {
				put.add(Bytes.toBytes(hbaseTab_cf), Bytes.toBytes("linkHref"), Bytes.toBytes(sFeedData.getLinkHref()));
			}
			if(sFeedData.getLinkTitle() != null) {
				put.add(Bytes.toBytes(hbaseTab_cf), Bytes.toBytes("linkTitle"), Bytes.toBytes(sFeedData.getLinkTitle()));
			}
			if(sFeedData.getContentValue() != null) {
				put.add(Bytes.toBytes(hbaseTab_cf), Bytes.toBytes("contentValue"), Bytes.toBytes(sFeedData.getContentValue()));
			}
			if(sFeedData.getContentType() != null) {
				put.add(Bytes.toBytes(hbaseTab_cf), Bytes.toBytes("contentType"), Bytes.toBytes(sFeedData.getContentType()));
			}
			if(sFeedData.getCategories() != null) {
				put.add(Bytes.toBytes(hbaseTab_cf), Bytes.toBytes("categories"), Bytes.toBytes(sFeedData.getCategories()));
			}
			if(sFeedData.getSummary() != null) {
				put.add(Bytes.toBytes(hbaseTab_cf), Bytes.toBytes("summary"), Bytes.toBytes(sFeedData.getSummary()));
			}
			
			hbaseAlpha.put(put);	
			//remeber this is important!
			hbaseAlpha.close();
	
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	//dumps data into betaFeed
	@SuppressWarnings("deprecation")
	public static void dumpFeedIntoBeta(FeedData sFeedData) {
		try {
			HTable hbaseBeta = new HTable(fetchHbaseConf(), RSSFeedUtils.betatable);
			String rowkey = sFeedData.getTitle() + "__" + sFeedData.getDescription();
			Put put = new Put(rowkey.getBytes());
			
			if(sFeedData.getRSSFeed() != null) {
				put.add(Bytes.toBytes(hbaseTab_cf), Bytes.toBytes("rssFeed"), Bytes.toBytes(sFeedData.getRSSFeed()));
			}
			if(sFeedData.getTitle() != null) {
				put.add(Bytes.toBytes(hbaseTab_cf), Bytes.toBytes("title"), Bytes.toBytes(sFeedData.getTitle()));
			}
			if(sFeedData.getLanguage() != null) {
				put.add(Bytes.toBytes(hbaseTab_cf), Bytes.toBytes("language"), Bytes.toBytes(sFeedData.getLanguage()));
			}
			if(sFeedData.getThumbnail() != null) {
				put.add(Bytes.toBytes(hbaseTab_cf), Bytes.toBytes("thumbnail"), Bytes.toBytes(sFeedData.getThumbnail()));
			}
			if(sFeedData.getArticleLink() != null) {
				put.add(Bytes.toBytes(hbaseTab_cf), Bytes.toBytes("articleLink"), Bytes.toBytes(sFeedData.getArticleLink()));
			}
			if(sFeedData.getDescription() != null) {
				put.add(Bytes.toBytes(hbaseTab_cf), Bytes.toBytes("description"), Bytes.toBytes(sFeedData.getDescription()));
			}
			if(sFeedData.getAuthor() != null) {
				put.add(Bytes.toBytes(hbaseTab_cf), Bytes.toBytes("author"), Bytes.toBytes(sFeedData.getAuthor()));
			}
			if(sFeedData.getPublishedDate() != null) {
				put.add(Bytes.toBytes(hbaseTab_cf), Bytes.toBytes("publishedDate"), Bytes.toBytes(sFeedData.getPublishedDate()));
				
				long ts = sFeedData.getPublishedDate();
				//get the article date and time from the epch timestamp in published date
				Date date = new Date(ts);
				String datestring = date.getDate()+"-"+(date.getMonth()+1) + "-" + (date.getYear()+1900) + " " + date.getHours() + 
									":" + date.getMinutes() + ":" + date.getSeconds();
				
				put.add(Bytes.toBytes(hbaseTab_cf), Bytes.toBytes("articleDate"), Bytes.toBytes(datestring));
				
			}
			if(sFeedData.getUpdatedDate() != null) {
				put.add(Bytes.toBytes(hbaseTab_cf), Bytes.toBytes("updatedDate"), Bytes.toBytes(sFeedData.getUpdatedDate()));
			}
			if(sFeedData.getLinkHref() != null) {
				put.add(Bytes.toBytes(hbaseTab_cf), Bytes.toBytes("linkHref"), Bytes.toBytes(sFeedData.getLinkHref()));
			}
			if(sFeedData.getLinkTitle() != null) {
				put.add(Bytes.toBytes(hbaseTab_cf), Bytes.toBytes("linkTitle"), Bytes.toBytes(sFeedData.getLinkTitle()));
			}
			if(sFeedData.getContentValue() != null) {
				put.add(Bytes.toBytes(hbaseTab_cf), Bytes.toBytes("contentValue"), Bytes.toBytes(sFeedData.getContentValue()));
			}
			if(sFeedData.getContentType() != null) {
				put.add(Bytes.toBytes(hbaseTab_cf), Bytes.toBytes("contentType"), Bytes.toBytes(sFeedData.getContentType()));
			}
			if(sFeedData.getCategories() != null) {
				put.add(Bytes.toBytes(hbaseTab_cf), Bytes.toBytes("categories"), Bytes.toBytes(sFeedData.getCategories()));
			}
			if(sFeedData.getSummary() != null) {
				put.add(Bytes.toBytes(hbaseTab_cf), Bytes.toBytes("summary"), Bytes.toBytes(sFeedData.getSummary()));
			}
			
			hbaseBeta.put(put);
			hbaseBeta.close();
			
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	//this makes the aylien api request to fetch categories and summary using combined calls as hashtag sugg.
	public static Map<String, String> addCategoryAndSummary(String articleLink) {
		Map<String, String> aylienmap = new HashMap<String, String>();
		try {
			StringBuilder ctgrybuildr = new StringBuilder("");
			StringBuilder summarybuildr = new StringBuilder("");
			CombinedParams.Builder builder = CombinedParams.newBuilder();
			String[] endpoints = {"hashtags", "summarize"};
			URL url = new URL(articleLink);
			builder.setUrl(url);
			builder.setEndpoints(endpoints);
			Combined combined = client.combined(builder.build());
			
			for(String hashtag : combined.getHashTags().getHashtags()) {
				ctgrybuildr.append(hashtag.replaceAll("#", "").trim() + ",");
			}
			
			if(ctgrybuildr.toString().matches("[a-zA-Z0-9]")) {
				ctgrybuildr = new StringBuilder(ctgrybuildr.toString().substring(0, ctgrybuildr.toString().length() -1));
			}
			
			for(String summary : combined.getSummary().getSentences()) {
				summarybuildr.append(summary.trim() + "__");
			}
			
			if(summarybuildr.toString().matches("[a-zA-Z0-9]")) {
				summarybuildr = new StringBuilder(summarybuildr.toString().substring(0, summarybuildr.toString().length() -2));
			}
			
			//put the values in a map
			aylienmap.put("categories", ctgrybuildr.toString());
			aylienmap.put("summary", summarybuildr.toString());
			
		} catch(MalformedURLException e) {
			e.printStackTrace();
		} catch(TextAPIException e) {
			e.printStackTrace();
		}
		return aylienmap;
	}
}
