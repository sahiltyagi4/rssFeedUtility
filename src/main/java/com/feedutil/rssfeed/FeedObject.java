package com.feedutil.rssfeed;

import java.util.Map;
import org.json.simple.JSONObject;

import com.feedutil.utils.RSSFeedUtils;

public class FeedObject {
	
	String rssFeed, title, language, articleLink, description, author, thumbnail;
	long publishedDate, updatedDate;
	String linkHrefStr, linkTitleStr, contentValStr, contentTypeStr, categories, summary;
	static int aylienctr = 0;
	
	public String getLinkHrefStr() {
		return linkHrefStr;
	}

	public void setLinkHrefStr(String linkHrefStr) {
		this.linkHrefStr = linkHrefStr;
	}

	public String getLinkTitleStr() {
		return linkTitleStr;
	}

	public void setLinkTitleStr(String linkTitleStr) {
		this.linkTitleStr = linkTitleStr;
	}

	public String getContentValStr() {
		return contentValStr;
	}

	public void setContentValStr(String contentValStr) {
		this.contentValStr = contentValStr;
	}

	public String getContentTypeStr() {
		return contentTypeStr;
	}

	public void setContentTypeStr(String contentTypeStr) {
		this.contentTypeStr = contentTypeStr;
	}

	public String getCategories() {
		return categories;
	}

	public void setCategories(String categories) {
		this.categories = categories;
	}

	public String getSummary() {
		return summary;
	}

	public void setSummary(String summary) {
		this.summary = summary;
	}

	public String getThumbnail() {
		return thumbnail;
	}

	public void setThumbnail(String thumbnail) {
		this.thumbnail = thumbnail;
	}
	
	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}
	
	public String getRssFeed() {
		return rssFeed;
	}

	public void setRssFeed(String rssFeed) {
		this.rssFeed = rssFeed;
	}
	
	public String getTitle() {
		return title;
	}
	
	public void setTitle(String title) {
		this.title = title;
	}
	
	public String getArticleLink() {
		return articleLink;
	}
	
	public void setArticleLink(String articleLink) {
		this.articleLink = articleLink;
	}
	
	public String getDescription() {
		return description;
	}
	
	public void setDescription(String description) {
		this.description = description;
	}
	
	public String getAuthor() {
		return author;
	}
	
	public void setAuthor(String author) {
		this.author = author;
	}
	
	public long getPublishedDate() {
		return publishedDate;
	}
	
	public void setPublishedDate(long publishedDate) {
		this.publishedDate = publishedDate;
	}
	
	public long getUpdatedDate() {
		return updatedDate;
	}

	public void setUpdateDate(long updateDate) {
		this.updatedDate = updateDate;
	}
	
	public static void processFeeds(FeedObject feedObject) {
		if(!FeedListener.checkList.contains(feedObject.getTitle().trim() + "__" + feedObject.getDescription().trim())) {
			
			//aylien api with using combined call for categories and summary...this runs only when we know a new feed object has been encountered and not a repeat
			Map<String, String> aylienMap = RSSFeedUtils.addCategoryAndSummary(feedObject.getArticleLink());
			aylienctr++;
			feedObject.setCategories(aylienMap.get("categories"));
			feedObject.setSummary(aylienMap.get("summary"));
			
			//feed obj converted to json obj
			JSONObject jsonObject = FeedProcessing.processFeedObject(feedObject);
			//dump to kafka after serialization using avro schema
			FeedProcessing.serializeIntoAvroObjects(jsonObject);
			//title and description used to identify a feed object
			FeedListener.checkList.add(feedObject.getTitle().trim() + "__" + feedObject.getDescription().trim());
			try {
				Thread.sleep(100);
			} catch(InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
}