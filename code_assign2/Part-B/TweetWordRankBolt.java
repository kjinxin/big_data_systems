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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.HashMap;

import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;	

@SuppressWarnings("serial")
public class TweetWordRankBolt extends BaseRichBolt {           
    OutputCollector _collector;
    Map<String, MyWord> wordcountmap;
    final static int TIME_INTERVAL_IN_SECONDS = 30;    

    public TweetWordRankBolt() {
	wordcountmap = new HashMap<String, MyWord>();
    }
     
    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
      this._collector = collector;      
    }
      
    
    @SuppressWarnings("unchecked")
    @Override
    public void execute(Tuple tuple) {

	if (TupleUtils.isTick(tuple)) {
		rankFilter();
	} else {
		String word = (String) tuple.getValueByField("word");
		if (wordcountmap.containsKey(word)) {
			wordcountmap.get(word).incrementCount();
		} else {
			wordcountmap.put(word, new MyWord(word));
		}		
	}
    }

    private void rankFilter() {
	List<MyWord> wordlist = new ArrayList<MyWord>();
	int wordnum = wordcountmap.size();

	for (Entry<String, MyWord> entry : wordcountmap.entrySet()) {
		wordlist.add(entry.getValue());
	}
	Collections.sort(wordlist);
	for (int i = 0; i < wordnum / 2; i ++) {
		_collector.emit("wordrankstream", new Values(wordlist.get(i).getWord(), wordlist.get(i).getCount()));
		System.out.println("rank " + wordlist.get(i).getWord() + "  " + wordlist.get(i).getCount());
	}
	wordcountmap.clear();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
	ofd.declareStream("wordrankstream", new Fields("word", "count"));		
    }    
    
    @Override
    public Map<String, Object> getComponentConfiguration() {
      Map<String, Object> conf = new HashMap<String, Object>();
      conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, TIME_INTERVAL_IN_SECONDS);
      return conf;
    }
}
