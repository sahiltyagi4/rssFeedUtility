package com.feedutil.rssfeed;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feedutil.utils.RSSFeedUtils;
import com.rometools.modules.mediarss.MediaEntryModule;
import com.rometools.modules.mediarss.types.Thumbnail;
import com.rometools.rome.feed.module.Module;
import com.rometools.rome.feed.synd.SyndCategory;
import com.rometools.rome.feed.synd.SyndContent;
import com.rometools.rome.feed.synd.SyndEntry;
import com.rometools.rome.feed.synd.SyndFeed;
import com.rometools.rome.feed.synd.SyndLink;
import com.rometools.rome.io.FeedException;
import com.rometools.rome.io.SyndFeedInput;
import com.rometools.rome.io.XmlReader;


public class FeedListener {
	private static long currtime = System.currentTimeMillis();
	public static List<String> checkList = new ArrayList<String>();
	SyndFeedInput sFeedInput = new SyndFeedInput();
	
	private void listenToRSSFeed(String url, long deleteBuffer) {
		try {
			//feed api to fetch data from feed url using rome api
			SyndFeed feed = sFeedInput.build(new XmlReader(new URL(url)));
			FeedObject feedObject;
			
			for(SyndEntry entry : (List<SyndEntry>)feed.getEntries()) {
				feedObject = new FeedObject();
				
				if(feed.getTitle() != null) {
					feedObject.setRssFeed(feed.getTitle());
				}
				if(feed.getLanguage() != null) {
					feedObject.setLanguage(feed.getLanguage());
				}
				if(entry.getTitle() != null) {
					feedObject.setTitle(entry.getTitle());
				}
				if(entry.getLink() != null) {
					feedObject.setArticleLink(entry.getLink());
				}
				if(entry.getDescription().getValue() != null) {
					feedObject.setDescription(entry.getDescription().getValue());
				}
				if(entry.getAuthor() != null) {
					feedObject.setAuthor(entry.getAuthor());
				}
				if(entry.getPublishedDate() != null) {
					feedObject.setPublishedDate(entry.getPublishedDate().getTime());
				} 
				//if there is no published date in feed object, then set it to current timestamp
				else if(entry.getPublishedDate() == null) {
					feedObject.setPublishedDate(System.currentTimeMillis());
				}
				if(entry.getUpdatedDate() != null) {
					feedObject.setUpdateDate(entry.getUpdatedDate().getTime());
				}
				
				
				List<String> linkHref = new LinkedList<String>();
				List<String> linkTitle = new LinkedList<String>();
				
				for(SyndLink slink : (List<SyndLink>)entry.getLinks()) {
					linkHref.add(slink.getHref());
					linkTitle.add(slink.getTitle());
				}
				
				//store only if some data is there...these are mostly blank in almost all feeds
				if(linkHref.size() > 0) {
					String linkHrefStr = convertListToString(linkHref);
					feedObject.setLinkHrefStr(linkHrefStr);
				}
				if(linkTitle.size() > 0) {
					String linkTitleStr = convertListToString(linkTitle);
					feedObject.setLinkTitleStr(linkTitleStr);
				}
				
				
				List<String> contentValue = new LinkedList<String>();
				List<String> contentType = new LinkedList<String>();
				
				for(SyndContent sContent : (List<SyndContent>)entry.getContents()) {
					contentValue.add(sContent.getValue());
					contentType.add(sContent.getType());
				}
				
				if(contentValue.size() > 0) {
					String contentValStr = convertListToString(contentValue);
					feedObject.setContentValStr(contentValStr);
				}
				if(contentType.size() > 0) {
					String contentTypeStr = convertListToString(contentType);
					feedObject.setContentTypeStr(contentTypeStr);
				}
				
				//this one collects the thumbnails and links to images
				for(Module module : (List<Module>)entry.getModules()) {
					if(module instanceof MediaEntryModule) {
						MediaEntryModule media = (MediaEntryModule)module;
						Thumbnail[] thumbarr = media.getMetadata().getThumbnail();
						if(thumbarr.length > 0) {
							for(Thumbnail thumb : thumbarr) {
								if(thumb.getUrl().toString() != null || !thumb.getUrl().toString().equals(null)) {
									feedObject.setThumbnail(thumb.getUrl().toString());
								}
							}
						}
					}
				}
				
				//serializes the feed object before storing in kafka. If feed object is new, add category and summary via aylien api
				FeedObject.processFeeds(feedObject);
			}
			
			//if delete buffer is reached, the feed objects stored in memory are flushed, right now it's set to five days
			long time = System.currentTimeMillis();
			if(time > (currtime + deleteBuffer)) {
				int index=0;
				while(index < checkList.size()) {
					checkList.remove(index);
					index++;
				}
				
				System.out.println("******************************* crossed one more cycle....");
				System.out.println("############################### alyien counter is at:" + FeedObject.aylienctr);
				currtime = System.currentTimeMillis();
			}
			
		} catch(IOException io) {
			io.printStackTrace();
		} catch(FeedException f) {
			f.printStackTrace();
		}
	}
	
	//the list object we made earlier is converted into comma delimited string
	private static String convertListToString(List<String> list) {
		Iterator<String> itr;
		String listStr = null;
		if(list != null) {
			listStr = "";
			itr = list.iterator();
			while(itr.hasNext()) {
				listStr = listStr + itr.next() + ", ";
			}
			listStr = listStr.substring(0, listStr.length() - 2);
		}
		return listStr;
	}
	
	public static void startFeed(long loopTime, long deleteBuffer, Set<String> urlList) {
		FeedListener feedlstnr = new FeedListener();
		Iterator<String> itr = urlList.iterator();
		while(itr.hasNext()) {
			String line = itr.next();
			//main method to listen and process feeds
			feedlstnr.listenToRSSFeed(line.trim(), deleteBuffer);
			
		}
		urlList.clear();
		try {
			System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ going to sleep for time set by looptime ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
			Thread.sleep(loopTime);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}