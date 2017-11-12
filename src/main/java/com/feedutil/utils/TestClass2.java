package com.feedutil.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TestClass2 {
	
	public static void main(String[] args) throws ParseException {
		String datetime = "14-7-2017 20:33:53";
		SimpleDateFormat df = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
		Date date = df.parse(datetime);
		System.out.println(date.getTime());
		
	}
	
}
