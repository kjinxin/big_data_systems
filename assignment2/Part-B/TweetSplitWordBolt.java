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

import java.util.HashSet;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

@SuppressWarnings("serial")
public class TweetSplitWordBolt extends BaseRichBolt {           
    OutputCollector _collector;
    HashSet<String> stopwords;
    
    public TweetSplitWordBolt(HashSet<String> stopwords) {	
	this.stopwords = stopwords;
    }
    
    public TweetSplitWordBolt() {
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
      this._collector = collector;      
    }
        
    @Override
    public void execute(Tuple tuple) {
	String tweet = (String) tuple.getValueByField("tweet");
	String words[] = tweet.split("\\s+");
	for (String word : words) {	    
	    String pureword = word.trim().toLowerCase();
	    // Remove '#' from hashtags
	    if (pureword.charAt(0) == '#') {
		pureword = pureword.substring(1);
	    }
	    if (!isStopWord(pureword)) {
		_collector.emit(new Values(pureword));		
		//System.out.println("Splitword" + pureword);
	    }
	}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
	ofd.declare(new Fields("word"));
    }    
    
    private boolean isStopWord(String word) {
	if (word.length() == 0) return true;
	if (stopwords.contains(word)) return true;
	for (int i = 0; i < word.length(); ++i) {
	    if (!Character.isAlphabetic(word.charAt(i))) {
		return true;
	    }
	}
	return false;
    }
}
