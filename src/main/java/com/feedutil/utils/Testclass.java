package com.feedutil.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class Testclass {
	
	public static void main(String[] args) {	
		scanDashboardTS(args[0]);
		
	}
	
	private static void scanDashboardTS(String table) {
		HConnection hConnection = null;
		HTableInterface hTableInterface = null;
		
		Configuration conf = RSSFeedUtils.fetchHbaseConf();
		try {
			hConnection = HConnectionManager.createConnection(conf);
			hTableInterface = hConnection.getTable(table);
			
			Scan scan = new Scan();
			ResultScanner scanner = hTableInterface.getScanner(scan);
			for(Result r = scanner.next(); r != null; r = scanner.next()) {
				try {
					Thread.sleep(500);
				} catch(InterruptedException e) {
					e.printStackTrace();
				}
				
				String rowkey = Bytes.toString(r.getRow());
				if(r.containsColumn(Bytes.toBytes("feedEvents_cf"), Bytes.toBytes("publishedDate"))) {
					String asd = Bytes.toString(r.getValue(Bytes.toBytes("feedEvents_cf"), Bytes.toBytes("title")));
					String pubdate = Bytes.toString(r.getValue(Bytes.toBytes("feedEvents_cf"), Bytes.toBytes("publishedDate")));
					System.out.println(pubdate + " AND title:" + asd);
					System.out.println("--------------------------------------------------------------------------------------------------------------");
				}
			}
			
			scanner.close();
		} catch(IOException e) {
			e.printStackTrace();
		}
		
		
		
	}
	
}
