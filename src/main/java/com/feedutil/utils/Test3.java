package com.feedutil.utils;

import java.util.Date;
import java.util.Map;

public class Test3 {
	public static void main(String[] args) {
		//String tags = "(America AND Cold War AND ISIS) OR (CIA AND Cold War) OR (CIA AND Russia AND Propaganda)";
		//String tags = "(EVENT D) OR (Delhi Assembly AND AAP) OR (AK AND Chief Minister) OR (Delhi AND Cabinet Ministers)";
		//String tags = "(Elections AND UK) OR (Theresa May) OR (Jeremy Corbyn AND EVENT 123) OR (Theresa May AND Brexit) OR (Coalition AND UK)";
		//String tags = "(Cricket AND UK) OR (Cricket AND England AND 123 AND EVENT Sahil Tyagi) OR (Joe Root) OR (County Cricket) OR (T20 AND UK)";
		//String tags = "(EVENT New Murray AND EVENT New Tennis AND Hingis)";
		String tags = "(EVENT New Tennis AND Hingis AND EVENT New Murray)";
		
//		Map<String, String> aylien = RSSFeedUtils.addCategoryAndSummary("http://www.bbc.co.uk/news/uk-england-40614960");
//		System.out.println(aylien.get("categories"));
		
		String backup = tags;
		if(backup.contains("EVENT")) {
			System.out.println("################################################################### yes it does");
		}
		String store = "alphaFeed";
		String s1 = "Adele fans will get booking fee refunded for cancelled shows";
		System.out.println("length of string is:" + s1.length());
		System.out.println(System.currentTimeMillis());
		while(tags.contains(")")) {
			String threstr=null;
			System.out.println("tags:" + tags.trim());
			
			String[] ortagstrings = tags.trim().split("OR");
			for(String ortag : ortagstrings) {
				System.out.println("val of ortag:" + ortag);
				threstr = ortag.trim();
				String qry ="SELECT rssFeed, title, articleLink, description, categories, publishedDate FROM " + store + " WHERE ";
				StringBuilder querybuilder = new StringBuilder(qry);
				
				//ortag = ortag.substring(0, ortag.indexOf(")"));
				System.out.println("tag:"+ortag.trim());
				
				String[] andtagstrings = ortag.trim().split("AND");
				System.out.println("size of arr:" + andtagstrings.length);
				
				String proctag=null;
				for(String processtag : andtagstrings) {
					System.out.println("process tag:" + processtag);
					proctag = processtag;
					if(proctag.trim().replaceAll("\\(", "").startsWith("EVENT")) {
						System.out.println("qwerty:" + proctag.trim());
						System.out.println("length of proctag:" + proctag.length());
						String curatedevent = proctag.trim().substring(6, proctag.trim().length()).trim().replaceAll(" ", "__").replaceAll("\\)", "");
						System.out.println("curated event:" + curatedevent);
						//dataset comes here
			
					} else if(!proctag.trim().replaceAll("\\(", "").startsWith("EVENT")) {
						querybuilder.append("categories RLIKE '" + proctag.trim().replaceAll("\\(", "").replaceAll("\\)", "") + "' AND ");
						//System.out.println("inner loop:" + querybuilder.toString());
					
					}
					
				}
				
				//if(!proctag.trim().replaceAll("\\(", "").contains("EVENT")) {
				
				if(querybuilder.toString().length() > qry.length()) {
					querybuilder = new StringBuilder(querybuilder.toString().substring(0, querybuilder.toString().length() -5));
					System.out.println("query is:" + querybuilder.toString());
					//dataset comes here
				}
				//}
				
				
			
			}
			
			
			tags = tags.substring(threstr.length(), tags.length()).trim().replaceAll("\\)", "");
		}
	
		//long t1 = System.currentTimeMillis();
		//Date date = new Date(t1);
		//System.out.println(date.getDate()+"-"+(date.getMonth()+1) + "-" + (date.getYear()+1900) + " " + date.getHours()+":"+date.getMinutes()+":"+date.getSeconds());
		
//		String proctag = "EVENT New Tennis";
//		String curatedevent = proctag.trim().substring(6, proctag.length()-1).trim().replaceAll(" ", "__").replaceAll("\\)", "");
//		System.out.println(curatedevent);
		
	}
}
