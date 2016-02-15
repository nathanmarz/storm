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
package org.apache.storm.starter.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.Consumer;
import org.apache.storm.trident.operation.Filter;
import org.apache.storm.trident.operation.FlatMapFunction;
import org.apache.storm.trident.operation.MapFunction;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;

/**
 * A simple example that demonstrates the usage of {@link org.apache.storm.trident.Stream#map(MapFunction)} and
 * {@link org.apache.storm.trident.Stream#flatMap(FlatMapFunction)} functions.
 */
public class TridentMapExample {

    private static MapFunction toUpper = new MapFunction() {
        @Override
        public Values execute(TridentTuple input) {
            return new Values(input.getStringByField("word").toUpperCase());
        }
    };

    private static FlatMapFunction split = new FlatMapFunction() {
        @Override
        public Iterable<Values> execute(TridentTuple input) {
            List<Values> valuesList = new ArrayList<>();
            for (String word : input.getString(0).split(" ")) {
                valuesList.add(new Values(word));
            }
            return valuesList;
        }
    };

    private static Filter theFilter = new BaseFilter() {
        @Override
        public boolean isKeep(TridentTuple tuple) {
            return tuple.getString(0).equals("THE");
        }
    };

    public static StormTopology buildTopology(LocalDRPC drpc) {
        FixedBatchSpout spout = new FixedBatchSpout(
                new Fields("word"), 3, new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"), new Values("four score and seven years ago"),
                new Values("how many apples can you eat"), new Values("to be or not to be the person"));
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();
        TridentState wordCounts = topology.newStream("spout1", spout).parallelismHint(16)
                .flatMap(split)
                .map(toUpper)
                .filter(theFilter)
                .peek(new Consumer() {
                    @Override
                    public void accept(TridentTuple input) {
                        System.out.println(input.getString(0));
                    }
                })
                .groupBy(new Fields("word"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .parallelismHint(16);

        topology.newDRPCStream("words", drpc)
                .flatMap(split)
                .groupBy(new Fields("args"))
                .stateQuery(wordCounts, new Fields("args"), new MapGet(), new Fields("count"))
                .filter(new FilterNull())
                .aggregate(new Fields("count"), new Sum(), new Fields("sum"));
        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        if (args.length == 0) {
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordCounter", conf, buildTopology(drpc));
            for (int i = 0; i < 100; i++) {
                System.out.println("DRPC RESULT: " + drpc.execute("words", "CAT THE DOG JUMPED"));
                Thread.sleep(1000);
            }
        } else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, buildTopology(null));
        }
    }
}
