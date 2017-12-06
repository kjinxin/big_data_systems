/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.starter;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

import org.apache.storm.starter.spout.PartBQ2Spout;
import org.apache.storm.starter.spout.SampleHashTagsSpout;
import org.apache.storm.starter.spout.SampleFriendCountSpout;
import org.apache.storm.starter.bolt.TweetFilterBolt;

import org.apache.storm.starter.bolt.TweetSplitWordBolt;
import org.apache.storm.starter.bolt.TweetWordRankBolt;


public class PartBQ2 {        
    public static void main(String[] args) {
        String consumerKey = "aF2fJ6VizMleZxLszjKDw7y1T"; 
        String consumerSecret = "SX8CYFM4Zl2aRhwskvcs8Uj0k8JKW0tGeS7dpZ8yxHfT3RT3yC"; 
        String accessToken = "1676214552-FfBlBXyeF4nsSBYrQFucnt7XOTWvvVh1WtIB0hP"; 
        String accessTokenSecret = "nkJNST0mHR4KsHJgIyEbVH9ozstVWgtwESLzjb6vIrXzJ";
	String topologyname = "PartBQ2";
	String mode = args[0];
	List<String> hashtags = new ArrayList<String>();
	List<Integer> friendcountlist = new ArrayList<Integer>();
	
	for (int i = 1; i < args.length; i ++) {
		try {
			friendcountlist.add(Integer.parseInt(args[i]));
		} catch (Exception e) {
			hashtags.add(args[i].toLowerCase());
		}
	}	

	// Load stopwords into a hashset
        HashSet<String> stopwords = new HashSet<String>();
        String line = null;
	try {
        	BufferedReader br = new BufferedReader(new FileReader("/home/ubuntu/software/apache-storm-1.0.2/examples/storm-starter/src/jvm/org/apache/storm/starter/stopwords.txt"));
		while ((line = br.readLine()) != null) {
		    stopwords.add(line.trim().toLowerCase());
		}
		br.close();
	} catch (Exception e) {
		e.printStackTrace();
	}

        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("twitter", new PartBQ2Spout(consumerKey, consumerSecret,
                                accessToken, accessTokenSecret, hashtags), 5);
	builder.setSpout("hashtagspout", new SampleHashTagsSpout(hashtags));
	builder.setSpout("friendcountspout", new SampleFriendCountSpout(friendcountlist));
	
	builder.setBolt("tweetfilterbolt", new TweetFilterBolt(hashtags), 5)
		.shuffleGrouping("twitter")
		.allGrouping("hashtagspout")
		.allGrouping("friendcountspout");
	
	builder.setBolt("tweetsplitbolt", new TweetSplitWordBolt(stopwords), 5)
		.shuffleGrouping("tweetfilterbolt");
	
	builder.setBolt("tweetwordrankbolt", new TweetWordRankBolt())
        		.allGrouping("tweetsplitbolt");

	//output tweets to hdfs
	String fsurl = "hdfs://10.254.0.64:8020";
	String fspath = "/user/ubuntu/PartBOutput/";
	RecordFormat format = new DelimitedRecordFormat();
	SyncPolicy syncPolicy = new CountSyncPolicy(1000);
	FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.GB);
	FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPrefix("PartBQ2Tweets").withPath(fspath);
			    
	HdfsBolt hdfsBolt = new HdfsBolt();
	hdfsBolt.withFsUrl(fsurl)
		 .withFileNameFormat(fileNameFormat)
		.withRecordFormat(format)
		.withSyncPolicy(syncPolicy)
		.withRotationPolicy(rotationPolicy);
	builder.setBolt("hdfsTweetsBolt", hdfsBolt).shuffleGrouping("tweetfilterbolt");	
		//builder.setBolt("writefiletolocal", new PartBQ1WriteLocalBolt(outputfilepath))
		//	.shuffleGrouping("twitter");


	//output top 50% words to hdfs
	fileNameFormat = new DefaultFileNameFormat().withPrefix("PartBQ2TopWords").withPath(fspath);
			    
	HdfsBolt hdfsWordBolt = new HdfsBolt();
	hdfsWordBolt.withFsUrl(fsurl)
		 .withFileNameFormat(fileNameFormat)
		.withRecordFormat(format)
		.withSyncPolicy(syncPolicy)
		.withRotationPolicy(rotationPolicy);
	builder.setBolt("hdfsWordBolt", hdfsWordBolt).shuffleGrouping("tweetwordrankbolt", "wordrankstream");	

	if (mode.equals("local")) {
		Config conf = new Config();
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(topologyname, conf, builder.createTopology());
		//Utils.sleep(10000);
		//cluster.shutdown();
		//while(true);
	} else {
		Config conf = new Config();
		conf.setNumWorkers(20);
		conf.setMaxSpoutPending(40000);
	        try {	
			StormSubmitter.submitTopology(topologyname, conf, builder.createTopology());
		} catch (Exception e) {
			e.printStackTrace();	
		}
	}
    }
}
