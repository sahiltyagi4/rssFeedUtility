package com.feedutil.rssfeed;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//main program to start collecting feeds
public class RSSFeedRunner {
	//zifferlabs main class to start loading and processing RSS Feeds
	
	private void startFeedListening(String[] args) throws IOException {
		//time duration at which the feed listener is to be triggered
		long loopTime = (Integer.parseInt(args[0])*60*1000);						//in minutes
		//time after which the feed objects info stored in memory gets deleted
		long deleteBuffer = (Integer.parseInt(args[1])*60*1000);					//in minutes
		
		while(true) {
			//read file for a feed provider, a line separated list of urls
			BufferedReader bfrdr = new BufferedReader(new InputStreamReader(new FileInputStream(new File(args[2]))));	//string value with config file location
			String url;
			Set<String> urlSet = new HashSet<String>();
			while((url = bfrdr.readLine()) != null) {
				urlSet.add(url.trim());
			}
			bfrdr.close();
			//load urls to set and start listening
			FeedListener.startFeed(loopTime, deleteBuffer, urlSet);
			urlSet.clear();
		}
	}
	
	public static void main(String[] args) throws IOException {
		RSSFeedRunner runner = new RSSFeedRunner();
		runner.startFeedListening(args);
	}
}
