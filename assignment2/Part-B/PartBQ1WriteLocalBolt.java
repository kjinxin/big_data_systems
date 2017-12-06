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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;


public class PartBQ1WriteLocalBolt extends BaseBasicBolt {
  private String outputfilepath;
  private int tweetcounter; 
  public PartBQ1WriteLocalBolt(String outputfilepath) {
    this.outputfilepath = outputfilepath;
    tweetcounter = 0;
    File outputfile = new File(outputfilepath);
    try {
      Files.deleteIfExists(outputfile.toPath());
    } catch(IOException e) {
      e.printStackTrace();
    }
  }
  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    String tweet = (String) tuple.getValueByField("tweet");
    try {
      BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputfilepath, true), "UTF-8"));
      bw.write(String.valueOf(tweetcounter + 1) + ". ");
      bw.write(tweet);
      bw.newLine();
      bw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }	
    System.out.println("The number of tweets printed: " + (++ tweetcounter));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer ofd) {
  }

}
