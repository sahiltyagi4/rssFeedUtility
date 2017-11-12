package com.feedutil.batch;

import java.io.Serializable;

public class DatasetBean implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String rssFeed;
	private String title;
	private String articleLink;
	private String description;
	private String categories;
	private String articleDate;
	//private String publishedDate;
	
	public String getArticleDate() {
		return articleDate;
	}
	public void setArticleDate(String articleDate) {
		this.articleDate = articleDate;
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
	public String getCategories() {
		return categories;
	}
	public void setCategories(String categories) {
		this.categories = categories;
	}
//	public String getPublishedDate() {
//		return publishedDate;
//	}
//	public void setPublishedDate(String publishedDate) {
//		this.publishedDate = publishedDate;
//	}
//	public static long getSerialversionuid() {
//		return serialVersionUID;
//	}
	

}
