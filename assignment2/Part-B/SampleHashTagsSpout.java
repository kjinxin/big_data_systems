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
package org.apache.storm.starter.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;
import java.util.ArrayList;
import java.util.List;

/**
 * Emits a random integer and a timestamp value (offset by one day),
 * every 100 ms. The ts field can be used in tuple time based windowing.
 */
public class SampleHashTagsSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private Random rand;
    
    static final int TIME_INTERVAL = 30000;
    private List<String> hashtags;

    public SampleHashTagsSpout(List<String> hashtags) {
    	this.hashtags = hashtags;
    }

    public SampleHashTagsSpout() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashtags"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.rand = new Random();
    }

    @Override
    public void nextTuple() {
        Utils.sleep(TIME_INTERVAL);
	int hashtagsnum = rand.nextInt(hashtags.size());
	if (hashtagsnum == 0) {
		hashtagsnum ++;
	}
	List<String> randhashtags = new ArrayList<String>();
	for (int i = 0; i < hashtagsnum; i ++) {
		String randhashtag = hashtags.get(rand.nextInt(hashtags.size()));
		if (randhashtags.contains(randhashtag)) {
			i --;
			continue;
		}
		randhashtags.add(randhashtag);
	}
        collector.emit(new Values(randhashtags));
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }
}
