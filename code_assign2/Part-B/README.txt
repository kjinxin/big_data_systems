PartBQ1:

Source code:

Main function: PartBQ1.java
Spout: 
	PartBQ1Spout.java 
	Use Twitter4j library to stream tweets in English language from twitter under the constriction of given keywords.
Bolt: 
	PartBQ1WriteLocalBolt.java(not used)
	Write to local file 
Bolt: 
	use HdfsBolt
	Write to hdfs

Compile application:
cd /home/ubuntu/software/apache-storm-1.0.2/examples/storm-starter
mvn clean install -DskipTests=true; mvn package -DskipTests=true

Run application:
cd /home/ubuntu/software/apache-storm-1.0.2/examples/storm-starter
storm jar target/storm-starter-*.jar org.apache.storm.starter.PartBQ1 <local/cluster> <keywords list>
e.g.: 
storm jar target/storm-starter-*.jar org.apache.storm.starter.PartBQ1 cluster halloween love peace Running win sex girls boys man woman father mother

Stop application:
In cluster model:
storm kill PartBQ1 or wait collecting 200000 tweets.
In local model:
storm kill PartBQ1


PartBQ2:

Source code:

Main function: PartBQ2.java
Spout:
	PartBQ2Spout.java
	Use Twitter4j library to stream tweets in English language from twitter under the constriction of hashtags.
	SampleFriendCountSpout.java
	In every 30 seconds, it will random sample a friendcount from the given friend count lists.
	SampleHashTagsSpout.java
	In every 30 seconds, it will random sample a list of hashtags based on the given hashtag lists.

Bolt:
	TweetFilterBolt.java
	Based on the stream received from PartBQ2Spout, SampleFriendCountSpout, SampleHashTagsSpout, use friendcount and hashtags to filter the tweets.
	TweetSplitWordBolt.java
	Based on the stream received from TweetFilterBolt, split the tweets into words, remove stopwords and remove non-character words.
	TweetWordRankBolt.java
	Based on the stream received from TweetSplitWordBolt, in every 30 seconds, it will sort the words received in current time interval and keep the words with rankings above 50%.

Helper class:
	MyWord.java
	To store word and its counter.

Compile application:
cd /home/ubuntu/software/apache-storm-1.0.2/examples/storm-starter
mvn clean install -DskipTests=true; mvn package -DskipTests=true

Run application:
cd /home/ubuntu/software/apache-storm-1.0.2/examples/storm-starter
storm jar target/storm-starter-*.jar org.apache.storm.starter.PartBQ2 <local/cluster> <friendcount list> <hashtags lisg>
e.g.:
storm jar target/storm-starter-*.jar org.apache.storm.starter.PartBQ2 cluster 5 6 7 8 9 10 20 40 60 80 100 1000 10000 100000 halloween love peace running win sex girls boys man woman father mother design sexy dress fashion free shop style sales today price pumpkin star iphone google airbnb

Stop application:
In cluster model:
storm kill PartBQ2 or wait collecting 200000 tweets.
In local model:
storm kill PartBQ2
