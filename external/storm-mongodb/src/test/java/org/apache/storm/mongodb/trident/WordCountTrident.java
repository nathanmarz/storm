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
package org.apache.storm.mongodb.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.mongodb.common.mapper.MongoMapper;
import org.apache.storm.mongodb.common.mapper.SimpleMongoMapper;
import org.apache.storm.mongodb.trident.state.MongoState;
import org.apache.storm.mongodb.trident.state.MongoStateFactory;
import org.apache.storm.mongodb.trident.state.MongoStateUpdater;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class WordCountTrident {

    public static StormTopology buildTopology(String url, String collectionName){
        Fields fields = new Fields("word", "count");
        FixedBatchSpout spout = new FixedBatchSpout(fields, 4,
                new Values("storm", 1),
                new Values("trident", 1),
                new Values("needs", 1),
                new Values("javadoc", 1)
        );
        spout.setCycle(true);

        MongoMapper mapper = new SimpleMongoMapper()
                .withFields("word", "count");

        MongoState.Options options = new MongoState.Options()
                .withUrl(url)
                .withCollectionName(collectionName)
                .withMapper(mapper);

        StateFactory factory = new MongoStateFactory(options);

        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newStream("spout1", spout);

        stream.partitionPersist(factory, fields,  new MongoStateUpdater(), new Fields());
        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(5);
        if (args.length == 2) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordCounter", conf, buildTopology(args[0], args[1]));
            Thread.sleep(60 * 1000);
            cluster.killTopology("wordCounter");
            cluster.shutdown();
            System.exit(0);
        }
        else if(args.length == 3) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[2], conf, buildTopology(args[0], args[1]));
        } else{
            System.out.println("Usage: WordCountTrident <mongodb url> <mongodb collection> [topology name]");
        }
    }

}
