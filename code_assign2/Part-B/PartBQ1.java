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

import org.apache.storm.starter.bolt.PartBQ1WriteLocalBolt;
import org.apache.storm.starter.spout.PartBQ1Spout;

public class PartBQ1 {        
    public static void main(String[] args) {
        String consumerKey = "aF2fJ6VizMleZxLszjKDw7y1T"; 
        String consumerSecret = "SX8CYFM4Zl2aRhwskvcs8Uj0k8JKW0tGeS7dpZ8yxHfT3RT3yC"; 
        String accessToken = "1676214552-FfBlBXyeF4nsSBYrQFucnt7XOTWvvVh1WtIB0hP"; 
        String accessTokenSecret = "nkJNST0mHR4KsHJgIyEbVH9ozstVWgtwESLzjb6vIrXzJ";
	String topologyname = "PartBQ1";
	String mode = args[0];
        String[] arguments = args.clone();
        String[] keyWords = Arrays.copyOfRange(arguments, 1, arguments.length);
        
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("twitter", new PartBQ1Spout(consumerKey, consumerSecret,
                                accessToken, accessTokenSecret, keyWords, mode, topologyname), 5);
	
	//if (outputfilepath.startsWith("hdfs")) {
	String fsurl = "hdfs://10.254.0.64:8020";
	String fspath = "/user/ubuntu/PartBOutput/";
	RecordFormat format = new DelimitedRecordFormat();
	SyncPolicy syncPolicy = new CountSyncPolicy(1000);
	FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.GB);
	FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPrefix("PartBQ1Tweets").withPath(fspath);
			    
	HdfsBolt hdfsBolt = new HdfsBolt();
	hdfsBolt.withFsUrl(fsurl)
		 .withFileNameFormat(fileNameFormat)
		.withRecordFormat(format)
		.withSyncPolicy(syncPolicy)
		.withRotationPolicy(rotationPolicy);
	builder.setBolt("hdfsBolt", hdfsBolt, 5).shuffleGrouping("twitter");	
	//} else {
	//	builder.setBolt("writefiletolocal", new PartBQ1WriteLocalBolt(outputfilepath))
	//		.shuffleGrouping("twitter");
	//}

	if (mode.equals("local")) {
		Config conf = new Config();
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(topologyname, conf, builder.createTopology());
		//Utils.sleep(10000);
		//cluster.shutdown();
	} else {
		Config conf = new Config();
		conf.setNumWorkers(20);
		conf.setMaxSpoutPending(4000);
	        try {	
			StormSubmitter.submitTopology(topologyname, conf, builder.createTopology());
		} catch (Exception e) {
			e.printStackTrace();	
		}
	}
    }
}
