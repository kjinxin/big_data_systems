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
package org.apache.storm.starter.bolt;


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;

import twitter4j.HashtagEntity;
import twitter4j.Status;

@SuppressWarnings("serial")
public class TweetFilterBolt extends BaseRichBolt {       
    OutputCollector _collector;    
    List<String> hashtags;
    int friendcount;
    final static int TIME_INTERVAL_IN_SECONDS = 30;    
    
    public TweetFilterBolt(List<String> hashtags) {
	this.hashtags = hashtags;
	this.friendcount = 1;
    }
    
    public TweetFilterBolt() {

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
      this._collector = collector;      
    }
    
    @Override
    public void execute(Tuple tuple) {
	if (tuple.getSourceComponent().equals("hashtagspout")) {	    
		hashtags.clear();
		hashtags.addAll((List<String>) tuple.getValue(0));
		Collections.sort(hashtags);
	} else if (tuple.getSourceComponent().equals("friendcountspout")) {	    
		friendcount = (Integer) tuple.getValue(0);	    
	} else {	    
		Status tweet = (Status) tuple.getValueByField("tweet");
		for (HashtagEntity hashtagEntity : tweet.getHashtagEntities()) {

			if (Collections.binarySearch(hashtags, hashtagEntity.getText().toLowerCase()) >= 0
				&& tweet.getUser().getFriendsCount() < friendcount) {
				_collector.emit(new Values(tweet.getText()));
			}
		}
	}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
	ofd.declare(new Fields("tweet"));
    }
}
