package com.feedutil.batch;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
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
import scala.util.parsing.combinator.testing.Str;

public class BatchProcessing implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private static Job feedevents;
	public static JavaSparkContext jsc;
	public static SparkSession sparkSession;
	public static SparkConf sparkConf;
	private static Configuration conf;
	public static String eventTable, includeORtags, includeANDtags, excludeORtags, excludeANDtags;
	public final static Logger logger = LoggerFactory.getLogger(BatchProcessing.class);
	public static List<Delete> keylist = new ArrayList<Delete>();
	//private static Map<String, Long> eventmap = new HashMap<String, Long>();
	private static String currentStore;
	
	public BatchProcessing() {
		RSSFeedUtils feedUtils = new RSSFeedUtils();
		//load spark props from utils class
		Map<String, String> sparkUtils = feedUtils.fetchSparkProperties();
		sparkConf = new SparkConf().setAppName(sparkUtils.get("appname"))
							.setMaster(sparkUtils.get("cores"))
							.set("spark.serializer", sparkUtils.get("spark_serializer"))
							.set("spark.driver.memory", sparkUtils.get("driverMemory"))
							.set("spark.streaming.blockInterval", sparkUtils.get("blockSize"))
							.set("spark.driver.allowMultipleContexts", "true")
							.set("spark.scheduler.mode", sparkUtils.get("scheduler"));

		jsc = new JavaSparkContext(sparkConf);
		//initiate a spark session for ziffer batch computation
		sparkSession = SparkSession.builder().appName("Zifferlabs batch processing")
					  .config("spark.scheduler.mode", "FAIR")
					  .config("spark.sql.crossJoin.enabled", "true")
					  .config("spark.sql.crossJoin.enabled", true)
					  .getOrCreate();
		
		//loads the HBase configuration
		conf = RSSFeedUtils.fetchHbaseConf();
		
	}
	
	@SuppressWarnings({ "deprecation", "serial" })
	private void doBatchCompute() {
		//use the betaFeed only
		HConnection hConnection = null;
		HTableInterface hTableInterface = null;
		HBaseAdmin admin = null;
		try {
			//scan the events table
			hConnection = HConnectionManager.createConnection(conf);
			hTableInterface = hConnection.getTable(DashboardUtils.curatedEvents);
			admin = new HBaseAdmin(conf);
			
			//create an empty dataset here and do union or intersections in subsequent nested iterations
			List<DatasetBean> emptyFeed = new ArrayList<DatasetBean>();
			DatasetBean datasetBean = new DatasetBean();
			
			//start scanning through the table
			Scan scan = new Scan();
			ResultScanner scanner = hTableInterface.getScanner(scan);
			for(Result r = scanner.next(); r != null; r = scanner.next()) {
				//to stop scanner from creating too many threads
				try {
					Thread.sleep(3000);
				} catch(InterruptedException e) {
					e.printStackTrace();
				}
				
				//cumulative set which is empty containing the DatasetBean class object
				Dataset<Row> cumulativeSet = sparkSession.createDataFrame(emptyFeed, datasetBean.getClass());
				
				//scan through every row of feedEvents table and process each corresponding event
				if(!r.isEmpty()) {
					eventTable = Bytes.toString(r.getRow());
					
					//create table if it didn't already exist
					if(!admin.tableExists(eventTable)) {
						HTableDescriptor creator = new HTableDescriptor(eventTable);
						creator.addFamily(new HColumnDescriptor(DashboardUtils.eventTab_cf));
						admin.createTable(creator);
						logger.info("**************** just created the following table in batch process since it didn't exist: " + eventTable);
					}
					
					//declare the dataset storing the data from betaFeed
					Dataset<Row> feedDataSet;
					//long eventdatacount = eventmap.get(eventTable);
					
					currentStore = RSSFeedUtils.betatable;
					//this dataset is populated with betaFeed data
					feedDataSet = loadbetaFeed();
					
					//store the data as a temporary table to process via sparkSQL
					feedDataSet.createOrReplaceTempView(currentStore);
					
					
					feedevents = Job.getInstance(conf);
					feedevents.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, eventTable);
					feedevents.setOutputFormatClass(TableOutputFormat.class);
					
					//read the tags attribute of the event, and start reading it from left to right....tags are in format as in the documentation
					//break the OR tag, followed by breaking the AND tag, followed by processing each tag or event contained in it
					String tags = "";
					if(r.containsColumn(Bytes.toBytes(DashboardUtils.curatedEvents_cf), Bytes.toBytes(DashboardUtils.curatedTags))) {
						tags = Bytes.toString(r.getValue(Bytes.toBytes(DashboardUtils.curatedEvents_cf), Bytes.toBytes(DashboardUtils.curatedTags)));
					}
					
					String backupTagStr = tags;
					
					while(tags.contains(")")) {
						String threstr=null;
						System.out.println("tags:" + tags.trim());
						
						String[] ortagstrings = tags.trim().split("OR");
						for(String ortag : ortagstrings) {
							System.out.println("val of ortag:" + ortag);
							threstr = ortag.trim();
							//these are the parameters being fetched and populated in the events...i.e, feedname,totle,link,description,categories and date
							String qry ="SELECT rssFeed, title, articleLink, description, categories, articleDate FROM " + currentStore + " WHERE ";
							StringBuilder querybuilder = new StringBuilder(qry);
							
							System.out.println("tag:"+ortag.trim());
							
							String[] andtagstrings = ortag.trim().split("AND");
							Dataset<Row> andSet = sparkSession.createDataFrame(emptyFeed, datasetBean.getClass());
							
							String proctag=null;
							for(int i=0; i<andtagstrings.length; i++) {
								proctag = andtagstrings[i];
								System.out.println("process tag:" + proctag);
								//if the part of the tag being processed is an event, open up a second stream to load data from corresponding table
								if(proctag.trim().replaceAll("\\(", "").startsWith("EVENT")) {
									System.out.println("qwerty:" + proctag.trim());
									String curatedevent = proctag.trim().substring(6, proctag.trim().length()).trim().replaceAll(" ", "__").replaceAll("\\)", "");
									logger.info("################################################################################# event:"+curatedevent);
									//dataset comes here
									if(admin.tableExists(curatedevent)) {
										Dataset<Row> eventdataset = loadcuratedEvent(curatedevent);
										logger.info("**************************************************** event:" + curatedevent + " while processing:"+eventTable);				
										
										if(i==0) {
											andSet = eventdataset.union(andSet);
										} else {
											
											if(andSet.count() == 0) {
												andSet = eventdataset.union(andSet);
											} else if(andSet.count() > 0) {
												andSet = eventdataset.intersect(andSet);
											}
										}
										
										
										
									} else {
										logger.info("*************************** event " + curatedevent + " does not exist *********************************");
									}
						
								//if it's a normal tag, make a sparkSQL query out of it
								} else if(!proctag.trim().replaceAll("\\(", "").startsWith("EVENT")) {
									querybuilder.append("categories RLIKE '" + proctag.trim().replaceAll("\\(", "").replaceAll("\\)", "") + "' AND ");
								
								}
								
							}
							
							
							//once the tag is fully processed, merge all data points and store in a resultant dataset
							if(querybuilder.toString().length() > qry.length()) {
								//logger.info("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ inside query string ###############################");
								querybuilder = new StringBuilder(querybuilder.toString().substring(0, querybuilder.toString().length() -5));
								//dataset comes here
								Dataset<Row> queryset = sparkSession.sql(querybuilder.toString());

								//id the set it empty, fill it with the single data point
								if(andSet.count() == 0 && !backupTagStr.contains("EVENT")) {
									andSet = queryset.union(andSet);
									
									logger.info("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ doing query string with zero count:" + eventTable);
									
								} else {
									
									logger.info("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ doing intersect for query:" + eventTable);
									
									andSet.createOrReplaceTempView("andSet1");
									queryset.createOrReplaceTempView("querySet1");
									
									Dataset<Row> fSet = sparkSession.sql("SELECT DISTINCT(a.*) FROM andSet1 a INNER JOIN querySet1 b ON a.title = b.title");
									
									andSet = fSet;
									queryset.unpersist();
									fSet.unpersist();
									
								}
							}
							
							cumulativeSet = andSet.union(cumulativeSet);
							andSet.unpersist();
							
						}
						
						tags = tags.substring(threstr.length(), tags.length()).trim().replaceAll("\\)", "");
					}
					
					logger.info("########################################################################################################### table:"+eventTable);
					
					cumulativeSet.createOrReplaceTempView("cumulativeEvent");
				
					//as a double check, only fetch distinct records from all the merges...done via sparkSQL
					Dataset<Row> finalSet = sparkSession.sql("SELECT DISTINCT(*) FROM cumulativeEvent");
					
					JavaRDD<Row> eventRDD = finalSet.toJavaRDD();
					JavaPairRDD<ImmutableBytesWritable, Put> eventpairRDD = eventRDD.mapToPair(new PairFunction<Row, ImmutableBytesWritable, Put>() {

						public Tuple2<ImmutableBytesWritable, Put> call(Row arg0) throws Exception {
							Object link = arg0.getAs("articleLink");
							if((String)link != null) {
								//parameters being populated into the events table
								return DashboardUtils.objectsofCuratedEvents(arg0.getAs("rssFeed"), arg0.getAs("title"), link, arg0.getAs("description"), 
																				arg0.getAs("categories"), arg0.getAs("articleDate"));
							} else {
								return DashboardUtils.objectsofCuratedEvents(arg0.getAs("rssFeed"), arg0.getAs("title"), "link not available", arg0.getAs("description"), 
																				arg0.getAs("categories"), arg0.getAs("articleDate"));
							}
							
						}
					});
					
					logger.info("******************************** going to dump curated events data in hbase for event: " + eventTable);
					eventpairRDD.saveAsNewAPIHadoopDataset(feedevents.getConfiguration());
					
					eventRDD.unpersist();
					finalSet.unpersist();
					cumulativeSet.unpersist();
				}
			}
			
			scanner.close();
			hTableInterface.close();
			hConnection.close();
			
		} catch(IOException e) {
			logger.info("error while establishing Hbase connection...");
			e.printStackTrace();
		}
	}
	
	//load beta feed as a dataset object
	private static Dataset<Row> loadbetaFeed() {
		conf.set(TableInputFormat.INPUT_TABLE, RSSFeedUtils.betatable);
		conf.set(TableInputFormat.SCAN_COLUMN_FAMILY, RSSFeedUtils.hbaseTab_cf);
		
		String feedCols = RSSFeedUtils.hbaseTab_cf + ":rssFeed " + RSSFeedUtils.hbaseTab_cf + ":title " + RSSFeedUtils.hbaseTab_cf + ":articleLink " 
						+ RSSFeedUtils.hbaseTab_cf + ":description " + RSSFeedUtils.hbaseTab_cf + ":articleDate " + RSSFeedUtils.hbaseTab_cf + ":categories";
		
		conf.set(TableInputFormat.SCAN_COLUMNS, feedCols);
		JavaPairRDD<ImmutableBytesWritable, Result> feedPairRDD = jsc.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
		
		JavaRDD<DatasetBean> feedRDD = feedPairRDD.map(new Function<Tuple2<ImmutableBytesWritable,Result>, DatasetBean>() {
			
			private static final long serialVersionUID = 1L;
			public DatasetBean call(Tuple2<ImmutableBytesWritable, Result> arg0) throws Exception {
				// TODO Auto-generated method stub
				Result r = arg0._2;
				//keylist.add(new Delete(r.getRow())); 
				
				DatasetBean databean = new DatasetBean();
				databean.setRssFeed(Bytes.toString(r.getValue(Bytes.toBytes(RSSFeedUtils.hbaseTab_cf), Bytes.toBytes("rssFeed"))));
				databean.setTitle(Bytes.toString(r.getValue(Bytes.toBytes(RSSFeedUtils.hbaseTab_cf), Bytes.toBytes("title"))));
				databean.setArticleLink(Bytes.toString(r.getValue(Bytes.toBytes(RSSFeedUtils.hbaseTab_cf), Bytes.toBytes("articleLink"))));
				databean.setDescription(Bytes.toString(r.getValue(Bytes.toBytes(RSSFeedUtils.hbaseTab_cf), Bytes.toBytes("description"))));
				databean.setCategories(Bytes.toString(r.getValue(Bytes.toBytes(RSSFeedUtils.hbaseTab_cf), Bytes.toBytes("categories"))));
				databean.setArticleDate(Bytes.toString(r.getValue(Bytes.toBytes(RSSFeedUtils.hbaseTab_cf), Bytes.toBytes("articleDate"))));
				
				return databean;
			}
		});
		
		Dataset<Row> feeddataset = sparkSession.createDataFrame(feedRDD, DatasetBean.class);
		return feeddataset;
	}
	
	//load the event as a dataset, if it exists
	@SuppressWarnings("serial")
	private static Dataset<Row> loadcuratedEvent(String curatedEvent) {
		conf.set(TableInputFormat.INPUT_TABLE, curatedEvent);
		conf.set(TableInputFormat.SCAN_COLUMN_FAMILY, DashboardUtils.curatedEvents_cf);
		
		String eventcols = DashboardUtils.curatedEvents_cf + ":rssFeed " + DashboardUtils.curatedEvents_cf + ":title " + DashboardUtils.curatedEvents_cf + 
							":link " + DashboardUtils.curatedEvents_cf + ":description " + DashboardUtils.curatedEvents_cf + ":categories " +
							DashboardUtils.curatedEvents_cf + ":articleDate";
		
		conf.set(TableInputFormat.SCAN_COLUMNS, eventcols);
		JavaPairRDD<ImmutableBytesWritable, Result> eventPairRDD = jsc.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
		
		//logger.info("`````````````````````````````````````````````````````````````````````````````````` event:" + curatedEvent);
		JavaRDD<DatasetBean> eventRDD = eventPairRDD.map(new Function<Tuple2<ImmutableBytesWritable,Result>, DatasetBean>() {

			public DatasetBean call(Tuple2<ImmutableBytesWritable, Result> arg0) throws Exception {
				// TODO Auto-generated method stub
				//using the Datasetbean class object to load the data from events table into spark dataset
				Result r = arg0._2;
				DatasetBean databean = new DatasetBean();			
				databean.setRssFeed(Bytes.toString(r.getValue(Bytes.toBytes(DashboardUtils.curatedEvents_cf), Bytes.toBytes("rssFeed"))));
				databean.setTitle(Bytes.toString(r.getValue(Bytes.toBytes(DashboardUtils.curatedEvents_cf), Bytes.toBytes("title"))));
				databean.setArticleLink(Bytes.toString(r.getValue(Bytes.toBytes(DashboardUtils.curatedEvents_cf), Bytes.toBytes("link"))));
				databean.setDescription(Bytes.toString(r.getValue(Bytes.toBytes(DashboardUtils.curatedEvents_cf), Bytes.toBytes("description"))));
				databean.setCategories(Bytes.toString(r.getValue(Bytes.toBytes(DashboardUtils.curatedEvents_cf), Bytes.toBytes("categories"))));
				databean.setArticleDate(Bytes.toString(r.getValue(Bytes.toBytes(DashboardUtils.curatedEvents_cf), Bytes.toBytes("articleDate"))));
				
				return databean;
			}
		});
		
		//eventmap.put(curatedEvent, eventRDD.count());
		Dataset<Row> eventdataset = sparkSession.createDataFrame(eventRDD, DatasetBean.class);
		return eventdataset;
	}
	
	@SuppressWarnings("deprecation")
	private void deleteBetaTable() {
		try {
			HTable betaTable = new HTable(RSSFeedUtils.fetchHbaseConf(), RSSFeedUtils.betatable);
			betaTable.delete(keylist);
			
			betaTable.close();
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		BatchProcessing proc = new BatchProcessing();
		
		//infinitely running batch computation
		while(true) {
			//where all event processing happens
			proc.doBatchCompute();
			
			try {
				//sleep for 30 minutes
				logger.info("############################################# completed another batch compute iteration...");
				Thread.sleep(1800000);
			} catch(InterruptedException interr) {
				interr.printStackTrace();
			}
		}
	}
}