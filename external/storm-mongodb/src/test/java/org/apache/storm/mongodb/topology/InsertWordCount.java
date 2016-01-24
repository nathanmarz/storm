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
package org.apache.storm.mongodb.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.mongodb.bolt.MongoInsertBolt;
import org.apache.storm.mongodb.common.mapper.MongoMapper;
import org.apache.storm.mongodb.common.mapper.SimpleMongoMapper;

import java.util.HashMap;
import java.util.Map;

public class InsertWordCount {
    private static final String WORD_SPOUT = "WORD_SPOUT";
    private static final String COUNT_BOLT = "COUNT_BOLT";
    private static final String INSERT_BOLT = "INSERT_BOLT";

    private static final String TEST_MONGODB_URL = "mongodb://127.0.0.1:27017/test";
    private static final String TEST_MONGODB_COLLECTION_NAME = "wordcount";
    

    public static void main(String[] args) throws Exception {
        Config config = new Config();

        String url = TEST_MONGODB_URL;
        String collectionName = TEST_MONGODB_COLLECTION_NAME;

        if (args.length >= 2) {
            url = args[0];
            collectionName = args[1];
        }

        WordSpout spout = new WordSpout();
        WordCounter bolt = new WordCounter();

        MongoMapper mapper = new SimpleMongoMapper()
                .withFields("word", "count");
        
        MongoInsertBolt insertBolt = new MongoInsertBolt(url, collectionName, mapper);

        // wordSpout ==> countBolt ==> MongoInsertBolt
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(WORD_SPOUT, spout, 1);
        builder.setBolt(COUNT_BOLT, bolt, 1).shuffleGrouping(WORD_SPOUT);
        builder.setBolt(INSERT_BOLT, insertBolt, 1).fieldsGrouping(COUNT_BOLT, new Fields("word"));


        if (args.length == 2) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", config, builder.createTopology());
            Thread.sleep(30000);
            cluster.killTopology("test");
            cluster.shutdown();
            System.exit(0);
        } else if (args.length == 3) {
            StormSubmitter.submitTopology(args[2], config, builder.createTopology());
        } else{
            System.out.println("Usage: InsertWordCount <mongodb url> <mongodb collection> [topology name]");
        }
    }
}
