package com.feedutil.rssfeed;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feedutil.schema.FeedData;
import com.feedutil.utils.RSSFeedUtils;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

@SuppressWarnings("deprecation")
public class FeedProcessing {
	
	static DatumWriter<FeedData> datumWriter = new SpecificDatumWriter<FeedData>(FeedData.class);
	
	@SuppressWarnings("unchecked")
	protected static JSONObject processFeedObject(FeedObject object) {
		JSONObject json = new JSONObject();
		
		if(object.getRssFeed() != null) {
			json.put("rssFeed", object.getRssFeed());
		}
		if(object.getTitle() != null) {
			json.put("title", object.getTitle());
		}
		if(object.getArticleLink() != null) {
			json.put("articleLink", object.getArticleLink());
		}
		if(object.getDescription() != null) {
			json.put("description", object.getDescription());
		}
		if(object.getAuthor() != null) {
			json.put("author", object.getAuthor());
		}
		if(object.getLanguage() != null) {
			json.put("language", object.getLanguage());
		}
		if(object.getThumbnail() != null) {
			json.put("thumbNail", object.getThumbnail());
		}
		if(object.getPublishedDate() > 0) {
			json.put("publishedDate", (long)object.getPublishedDate());
		}
		if(object.getUpdatedDate() > 0) {
			json.put("updatedDate", (long)object.getUpdatedDate());
		}
		
		if(object.getLinkHrefStr() != null) {
			json.put("linkHref", object.getLinkHrefStr());
		}
		if(object.getLinkTitleStr() != null) {
			json.put("linkTitle", object.getLinkTitleStr());
		}
		if(object.getContentValStr() != null) {
			json.put("contentValue", object.getContentValStr());
		}
		if(object.getContentTypeStr() != null) {
			json.put("contentType", object.getContentTypeStr());
		}
		if(object.getCategories() != null) {
			json.put("categories", object.getCategories());
		}
		if(object.getSummary() != null) {
			json.put("summary", object.getSummary());
		}
		
		return json;
	}
	
	@SuppressWarnings("unchecked")
	protected static void serializeIntoAvroObjects(JSONObject jsonObject) {
		FeedData data = new FeedData();
		//taking the json object and converting it to feeddata class object
		data.setRSSFeed((String)jsonObject.get("rssFeed"));
		data.setTitle((String)jsonObject.get("title"));
		data.setLanguage((String)jsonObject.get("language"));
		data.setThumbnail((String)jsonObject.get("thumbNail"));
		data.setArticleLink((String)jsonObject.get("articleLink"));
		data.setDescription((String)jsonObject.get("description"));
		data.setAuthor((String)jsonObject.get("author"));
		data.setPublishedDate((Long)jsonObject.get("publishedDate"));
		data.setUpdatedDate((Long)jsonObject.get("updatedDate"));
		data.setLinkHref((String)jsonObject.get("linkHref"));
		data.setLinkTitle((String)jsonObject.get("linkTitle"));
		data.setContentValue((String)jsonObject.get("contentValue"));
		data.setContentType((String)jsonObject.get("contentType"));
		data.setCategories((String)jsonObject.get("categories"));
		data.setSummary((String)jsonObject.get("summary"));
		
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
		try {
			datumWriter.write(data, encoder);
			encoder.flush();
			outputStream.close();
			
			//finally this method dumps data into kafka into the topic specified
			dumpIntoKafka(outputStream.toByteArray());
			
		} catch(IOException io) {
			io.printStackTrace();
		}
	}
	
	//dumps stream to kafka node
	private static void dumpIntoKafka(byte[] bytearray) {
		Properties props = RSSFeedUtils.fetchKafkaProperties();
		ProducerConfig pConfig = new ProducerConfig(props);
		Producer<String, byte[]> producer = new Producer<String, byte[]>(pConfig);
		KeyedMessage<String, byte[]> message = new KeyedMessage<String, byte[]>(RSSFeedUtils.kafkaTopic, bytearray);
		producer.send(message);
		producer.close();
	}
}
