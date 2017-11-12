package com.feedutil.kafka;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.minlog.Log;
import com.feedutil.schema.FeedData;
import com.feedutil.schema.SerializableFeedData;
import com.feedutil.utils.RSSFeedUtils;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

public class KafkaFeedConsumer {
	
	private void initiateKafkaConsumer() {
		//load kafka props from utils class
		Properties props = RSSFeedUtils.fetchKafkaProperties();
		ConsumerConfig config = new ConsumerConfig(props);
		ConsumerConnector connector = Consumer.createJavaConsumerConnector(config);
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(RSSFeedUtils.kafkaTopic, RSSFeedUtils.numThreads);
		
		Map<String, List<KafkaStream<byte[], byte[]>>> conMap = connector.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> stream = conMap.get(RSSFeedUtils.kafkaTopic);
		if(stream.size() > 0) {
			//start a consumer to listen to given topic and extract records
			readAvroObjects(stream.get(0));
		}
	}
	
	private static void readAvroObjects(KafkaStream<byte[], byte[]> stream) {
		ConsumerIterator<byte[], byte[]> itr = stream.iterator();
		DatumReader<FeedData> datumReader = new SpecificDatumReader<FeedData>(FeedData.getClassSchema());
		ByteArrayInputStream byInputStream = null;
		FeedData feedData = null;
		while(itr.hasNext()) {
			MessageAndMetadata<byte[], byte[]> messageAndMetadata = itr.next();
			byInputStream = new ByteArrayInputStream(messageAndMetadata.message());
			Decoder decoder = DecoderFactory.get().binaryDecoder(messageAndMetadata.message(), null);
			try {
				feedData = datumReader.read(null, decoder);
				//populate the alpha table
				RSSFeedUtils.dumpFeedIntoAlpha(feedData);
				//populate the beta table
				RSSFeedUtils.dumpFeedIntoBeta(feedData);
				byInputStream.close();
			} catch(IOException e) {
				e.printStackTrace();
			}	
		}
	}
	
	public static void main(String[] args) {
		KafkaFeedConsumer object = new KafkaFeedConsumer();
		object.initiateKafkaConsumer();
	}
}
