package com.feedutil.schema;

import java.io.IOException;
import java.io.Serializable;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

public class SerializableFeedData extends FeedData implements Serializable {
	private void setValues(FeedData obj) {
		setRSSFeed(obj.getRSSFeed());
		setTitle(obj.getTitle());
		setLanguage(obj.getLanguage());
		setThumbnail(obj.getThumbnail());
		setArticleLink(obj.getArticleLink());
		setDescription(obj.getDescription());
		setAuthor(obj.getAuthor());
		setPublishedDate(obj.getPublishedDate());
		setUpdatedDate(obj.getUpdatedDate());
		setLinkHref(obj.getLinkHref());
		setLinkTitle(obj.getLinkTitle());
		setContentType(obj.getContentType());
		setContentValue(obj.getContentValue());
		setCategories(obj.getCategories());
	}
	
	public SerializableFeedData(FeedData obj) {
		setValues(obj);
	}
	
	private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        DatumWriter<FeedData> writer = new SpecificDatumWriter<FeedData>(FeedData.class);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(this, encoder);
        encoder.flush();
    }
	
	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        DatumReader<FeedData> reader = new SpecificDatumReader<FeedData>(FeedData.class);
        Decoder decoder = DecoderFactory.get().binaryDecoder(in, null);
        setValues(reader.read(null, decoder));
    }

}