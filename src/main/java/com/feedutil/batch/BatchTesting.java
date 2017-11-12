package com.feedutil.batch;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feedutil.utils.DashboardUtils;
import com.feedutil.utils.RSSFeedUtils;

import scala.Tuple2;

public class BatchTesting implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public static JavaSparkContext jsc;
	public static SparkSession sparkSession;
	public static SparkConf sparkConf;
	private static Configuration conf;
	public static String eventTable, includeTags;
	private final static Logger logger = LoggerFactory.getLogger(BatchProcessing.class);
	
	public BatchTesting() {
		RSSFeedUtils feedUtils = new RSSFeedUtils();
		Map<String, String> sparkUtils = feedUtils.fetchSparkProperties();
		sparkConf = new SparkConf().setAppName(sparkUtils.get("appname"))
							.setMaster(sparkUtils.get("cores"))
							.set("spark.serializer", sparkUtils.get("spark_serializer"))
							.set("spark.driver.memory", sparkUtils.get("driverMemory"))
							.set("spark.streaming.blockInterval", sparkUtils.get("blockSize"))
							.set("spark.driver.allowMultipleContexts", "true")
							.set("spark.scheduler.mode", sparkUtils.get("scheduler"));

		jsc = new JavaSparkContext(sparkConf);
		sparkSession = SparkSession.builder().appName("Zifferlabs batch processing")
					  .config("spark.scheduler.mode", "FAIR")
					  .config("spark.sql.crossJoin.enabled", "true")
					  .config("spark.sql.crossJoin.enabled", true)
					  .getOrCreate();
		
		conf = RSSFeedUtils.fetchHbaseConf();
		
	}
	
	private static Dataset<Row> loadalphaFeed() {
		conf.set(TableInputFormat.INPUT_TABLE, RSSFeedUtils.alphatable);
		conf.set(TableInputFormat.SCAN_COLUMN_FAMILY, RSSFeedUtils.hbaseTab_cf);
		
		String alphaFeedCols = RSSFeedUtils.hbaseTab_cf + ":rssFeed " + RSSFeedUtils.hbaseTab_cf + ":title " + RSSFeedUtils.hbaseTab_cf + ":language "
							+ RSSFeedUtils.hbaseTab_cf + ":articleLink " + RSSFeedUtils.hbaseTab_cf + ":description " + RSSFeedUtils.hbaseTab_cf + ":author "
							+ RSSFeedUtils.hbaseTab_cf + ":thumbnail " + RSSFeedUtils.hbaseTab_cf + ":publishedDate " + RSSFeedUtils.hbaseTab_cf + ":updatedDate "
							+ RSSFeedUtils.hbaseTab_cf + ":linkHref " + RSSFeedUtils.hbaseTab_cf + ":linkTitle " + RSSFeedUtils.hbaseTab_cf + ":contentType "
							+ RSSFeedUtils.hbaseTab_cf + ":contentValue " + RSSFeedUtils.hbaseTab_cf + ":categories";
		
		conf.set(TableInputFormat.SCAN_COLUMNS, alphaFeedCols);
		JavaPairRDD<ImmutableBytesWritable, Result> feedPairRDD = jsc.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
		
		JavaRDD<BatchDataBean> feedRDD = feedPairRDD.map(new Function<Tuple2<ImmutableBytesWritable,Result>, BatchDataBean>() {
			
			private static final long serialVersionUID = 1L;
			public BatchDataBean call(Tuple2<ImmutableBytesWritable, Result> arg0) throws Exception {
				// TODO Auto-generated method stub
				Result r = arg0._2;
				//String rowKey = Bytes.toString(r.getRow());
				
				BatchDataBean databean = new BatchDataBean();
				databean.setRssFeed(Bytes.toString(r.getValue(Bytes.toBytes(RSSFeedUtils.hbaseTab_cf), Bytes.toBytes("rssFeed"))));
				databean.setTitle(Bytes.toString(r.getValue(Bytes.toBytes(RSSFeedUtils.hbaseTab_cf), Bytes.toBytes("title"))));
				databean.setLanguage(Bytes.toString(r.getValue(Bytes.toBytes(RSSFeedUtils.hbaseTab_cf), Bytes.toBytes("language"))));
				databean.setArticleLink(Bytes.toString(r.getValue(Bytes.toBytes(RSSFeedUtils.hbaseTab_cf), Bytes.toBytes("articleLink"))));
				databean.setDescription(Bytes.toString(r.getValue(Bytes.toBytes(RSSFeedUtils.hbaseTab_cf), Bytes.toBytes("description"))));
				databean.setAuthor(Bytes.toString(r.getValue(Bytes.toBytes(RSSFeedUtils.hbaseTab_cf), Bytes.toBytes("author"))));
				databean.setThumbnail(Bytes.toString(r.getValue(Bytes.toBytes(RSSFeedUtils.hbaseTab_cf), Bytes.toBytes("thumbnail"))));
				databean.setLinkHref(Bytes.toString(r.getValue(Bytes.toBytes(RSSFeedUtils.hbaseTab_cf), Bytes.toBytes("linkHref"))));
				databean.setLinkTitle(Bytes.toString(r.getValue(Bytes.toBytes(RSSFeedUtils.hbaseTab_cf), Bytes.toBytes("linkTitle"))));
				databean.setContentVal(Bytes.toString(r.getValue(Bytes.toBytes(RSSFeedUtils.hbaseTab_cf), Bytes.toBytes("contentValue"))));
				databean.setContentType(Bytes.toString(r.getValue(Bytes.toBytes(RSSFeedUtils.hbaseTab_cf), Bytes.toBytes("contentType"))));
				databean.setCategories(Bytes.toString(r.getValue(Bytes.toBytes(RSSFeedUtils.hbaseTab_cf), Bytes.toBytes("categories"))));
				databean.setPublishedDate(Bytes.toLong(r.getValue(Bytes.toBytes(RSSFeedUtils.hbaseTab_cf), Bytes.toBytes("publishedDate"))));
				if(Bytes.toString(r.getValue(Bytes.toBytes(RSSFeedUtils.hbaseTab_cf), Bytes.toBytes("updatedDate"))) != null) {
					databean.setUpdatedDate(Bytes.toLong(r.getValue(Bytes.toBytes(RSSFeedUtils.hbaseTab_cf), Bytes.toBytes("updatedDate"))));
				} else {
					databean.setUpdatedDate(0L);
				}
				
				return databean;
			}
		});
		
		//logger.info("-------------------------------- rdd count is: " + feedRDD.count());
		Dataset<Row> alphaDS = sparkSession.createDataFrame(feedRDD, BatchDataBean.class);
		return alphaDS;
	}
	
	@SuppressWarnings("deprecation")
	private void doBatchCompute() {
		/*Dataset<Row> alphaDataSet = loadalphaFeed();
		alphaDataSet.createOrReplaceTempView(RSSFeedUtils.alphatable);
		alphaDataSet.cache();
		
		Dataset<Row> ds = sparkSession.sql("Select * from alphaFeed");
		logger.info("******************************************** distinct count: " + ds.distinct().count() + "  $$$$$$$$$$$$$$$$$$$$$ total count: "+ds.count());
		
		List<BatchDataBean> list = new ArrayList<BatchDataBean>();
		Dataset<Row> ds2 = sparkSession.createDataFrame(list, BatchDataBean.class);
		
		//Dataset<Row> ds3 = ds2.unionAll(ds);
		ds2 = ds2.unionAll(ds);
		logger.info("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@  union count of ds3: " + ds2.count());*/
		
		/*HConnection hConnection = null;
		HTableInterface hTableInterface = null;
		HBaseAdmin admin = null;
		try {
			hConnection = HConnectionManager.createConnection(conf);
			hTableInterface = hConnection.getTable(DashboardUtils.curatedEvents);
			admin = new HBaseAdmin(conf);
			
			Scan scan = new Scan();
			ResultScanner scanner = hTableInterface.getScanner(scan);
			for(Result r = scanner.next(); r != null; r = scanner.next()) {
				if(!r.isEmpty()) {
					eventTable = Bytes.toString(r.getRow());
					
					if(!admin.tableExists(eventTable)) {
						HTableDescriptor creator = new HTableDescriptor(eventTable);
						creator.addFamily(new HColumnDescriptor(DashboardUtils.eventTab_cf));
						admin.createTable(creator);
						logger.info("**************** just created the following table in batch process since it didn't exist: " + eventTable);
					}
					
					//add part to truncate table here in every iteration
					//admin.truncateTable(TableName.valueOf(eventTable), false);
					
					feedevents = Job.getInstance(conf);
					feedevents.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, eventTable);
					feedevents.setOutputFormatClass(TableOutputFormat.class);
					
					if(r.containsColumn(Bytes.toBytes(DashboardUtils.curatedEvents_cf), Bytes.toBytes(DashboardUtils.qualifier))) {
						includeTags = Bytes.toString(r.getValue(Bytes.toBytes(DashboardUtils.curatedEvents_cf), Bytes.toBytes(DashboardUtils.qualifier)));*/
						
						//Dataset<Row> eventDataSet = sparkSession.sql("SELECT rssFeed, title, articleLink, description, categories FROM alphaFeed "
							//					+ "WHERE categories RLIKE '" + includeTags + "' OR description RLIKE '" + includeTags + "' AND categories IS NOT NULL");
						
						//ds.show(50);
						
					/*	JavaRDD<Row> eventRDD = eventDataSet.javaRDD();
						if(eventRDD.count() > 0) {
							JavaPairRDD<ImmutableBytesWritable , Put> eventPairRDD = eventRDD.mapToPair(new PairFunction<Row, ImmutableBytesWritable, Put>() {
								private static final long serialVersionUID = 1L;

								public Tuple2<ImmutableBytesWritable, Put> call(Row arg0) throws Exception {
									// TODO Auto-generated method stub
									return DashboardUtils.processCuratedEvents(arg0.getString(0), arg0.getString(1), arg0.getString(2), arg0.getString(3), 
																				arg0.getString(4));
								}
							});
							
							logger.info("******************************** going to dump curated events data in hbase for event: " + eventTable);
							logger.info("******************************** corresponding count for event table is: " + eventPairRDD.count());
							eventPairRDD.saveAsNewAPIHadoopDataset(feedevents.getConfiguration());*/
							
						//}
				//	}
			//	}
		//	}
			//scanner.close();
			
	//	} catch(IOException e) {
		//	logger.info("XXXXXXXXXXXXXXXXXXXXXXXXXX   error while establishing Hbase connection   XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
		//	e.printStackTrace();
		//}
		
		try {
			String store = null;
			HConnection hConnection = null;
			HBaseAdmin admin = null;
			hConnection = HConnectionManager.createConnection(conf);
			HTableInterface evntinterface = hConnection.getTable("London");
			
			
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		BatchTesting batchTesting = new BatchTesting();
		//while(true) {
			
			batchTesting.doBatchCompute();
			
		//}
	}
}