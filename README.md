# rssFeedUtility
RSS feed aggregator to create events to fetch relevant  news articles stored in the system, analyze the content and generate key hashtags and summary of the articles. Using Spark, Avro, HBase + HDFS and Kafka.

This repo constitutes a feed aggregator subscribing to various rss news feeds. The links are added to external files and the system starts subscribing to the feeds. The feed data is published to a message broker, Kafka in this case. A batch process in Spark subscribes to the same topic and runs algorithms for processing like summary creation, key tags, hashtags, etc. The results are stored in Hbase.

I've also built-in a dashboard API to allow users to create custom events and tags (which may contain multiple attributes) and associate other existing events with new ones. This way, a user can retrieve any # of articles associated with the event in reverse chronological order. Examples of some ways the curated events can be created:

Event: Donald Trump's Russian links | Tags: (Donald Trump AND Russia) OR (Donald Trump AND Putin) OR (Donal Trump AND Leaks AND White House) OR (Michael Flynn allegations)

Event: ISIS - a Terror | Tags: (America AND Cold war AND ISIS) OR (CIA AND Cold War) OR (CIA AND Russia AND Propaganda)

Event: ISIS vs The World | Tags: (Event Donald Trump's Russian links AND ISIS - a Terror) OR (ISIS)

The events created by the users are picked up in the batch job where the individual attributes are segregated and matched against the feed articles. The sources to read from the pipe are two: old news articles stored on-disk and new articles streaming over the message broker. If an article corresponding to an event is processed once, it is not processed again to conserve compute resources.

##Build the project using Maven with: mvn cleam compile assembly:single

Here are the commands to start various services:
Command to start feed listener:
###FEED LISTENER:
user=root
java -cp /home/ubuntu/rssFeedUtility/target/ZifferLabs-1-jar-with-dependencies.jar com.feedutil.rssfeed.RSSFeedRunner "*loop time*" "*delete buffer*" "**path to .txt config file**"
Example:
java -cp /home/ubuntu/rssFeedUtility/target/ZifferLabs-1-jar-with-dependencies.jar com.feedutil.rssfeed.RSSFeedRunner "60" "7200" "/home/ubuntu/rssFeedUtility/BBCsample.txt"

###MESSAGE BROKER:
Command to start consumer to dump feed data to hbase:
user=hdfs
java -cp /home/ubuntu/rssFeedUtility/target/ZifferLabs-1-jar-with-dependencies.jar com.feedutil.kafka.KafkaFeedConsumer

###BATCH PROCESSING:
Command to start batch processing:
user=hdfs
location=/etc/spark/spark-2.0.0-bin-hadoop2.3
./bin/spark-submit --jars /home/ubuntu/rssFeedUtility/target/ZifferLabs-1-jar-with-dependencies.jar --class "com.feedutil.batch.BatchProcessing" /home/ubuntu/rssFeedUtility/src/main/java/com/feedutil/batch/BatchProcessing.java

##DASHBOARD API:
Dashboard URL to create a curated event:
http://IP:PORT/addevent?event=*EVENT name*&tags=*associated tags*
Example:
127.0.0.1:8082/addevent?event=New Murray and Hingis at Wimbledon&tags=(Murray AND Tennis AND Hingis)

URL to fetch articles for the curated event:
http://IP:PORT/fetchevent?event=*EVENT name*&records=*num of rec*
Example:
127.0.0.1:8082/fetchevent?event=New Murray and Hingis at Wimbledon&records=3