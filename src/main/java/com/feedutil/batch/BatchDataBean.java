package com.feedutil.batch;

import java.io.Serializable;

public class BatchDataBean implements Serializable {
	private String rssFeed;
	private String title;
	private String language;
	private String articleLink;
	private String description;
	private String author;
	private String thumbnail;
	private String linkHref;
	private String linkTitle;
	private String contentVal;
	private String contentType;
	private String categories;
	private String summary;
	private Long publishedDate;
	private Long updatedDate;
	
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
	public String getLanguage() {
		return language;
	}
	public void setLanguage(String language) {
		this.language = language;
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
	public String getThumbnail() {
		return thumbnail;
	}
	public void setThumbnail(String thumbnail) {
		this.thumbnail = thumbnail;
	}
	public String getLinkHref() {
		return linkHref;
	}
	public void setLinkHref(String linkHref) {
		this.linkHref = linkHref;
	}
	public String getLinkTitle() {
		return linkTitle;
	}
	public void setLinkTitle(String linkTitle) {
		this.linkTitle = linkTitle;
	}
	public String getContentVal() {
		return contentVal;
	}
	public void setContentVal(String contentVal) {
		this.contentVal = contentVal;
	}
	public String getContentType() {
		return contentType;
	}
	public void setContentType(String contentType) {
		this.contentType = contentType;
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
	public Long getPublishedDate() {
		return publishedDate;
	}
	public void setPublishedDate(Long publishedDate) {
		this.publishedDate = publishedDate;
	}
	public Long getUpdatedDate() {
		return updatedDate;
	}
	public void setUpdatedDate(Long updatedDate) {
		this.updatedDate = updatedDate;
	}
	
}
