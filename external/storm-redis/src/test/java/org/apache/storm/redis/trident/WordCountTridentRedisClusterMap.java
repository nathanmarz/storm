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
package org.apache.storm.redis.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.TupleMapper;
import org.apache.storm.redis.trident.state.RedisClusterMapState;
import org.apache.storm.redis.common.config.JedisClusterConfig;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.testing.FixedBatchSpout;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

public class WordCountTridentRedisClusterMap {
    public static StormTopology buildTopology(String redisHostPort){
        Fields fields = new Fields("word", "count");
        FixedBatchSpout spout = new FixedBatchSpout(fields, 4,
                new Values("storm", 1),
                new Values("trident", 1),
                new Values("needs", 1),
                new Values("javadoc", 1)
        );
        spout.setCycle(true);

        Set<InetSocketAddress> nodes = new HashSet<InetSocketAddress>();
        for (String hostPort : redisHostPort.split(",")) {
            String[] host_port = hostPort.split(":");
            nodes.add(new InetSocketAddress(host_port[0], Integer.valueOf(host_port[1])));
        }
        JedisClusterConfig clusterConfig = new JedisClusterConfig.Builder().setNodes(nodes)
                                        .build();
        RedisDataTypeDescription dataTypeDescription = new RedisDataTypeDescription(
                RedisDataTypeDescription.RedisDataType.HASH, "test");
        StateFactory factory = RedisClusterMapState.transactional(clusterConfig, dataTypeDescription);

        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newStream("spout1", spout);

        TridentState state = stream.groupBy(new Fields("word"))
                .persistentAggregate(factory, new Fields("count"), new Sum(), new Fields("sum"));

        stream.stateQuery(state, new Fields("word"), new MapGet(), new Fields("sum"))
                .each(new Fields("word", "sum"), new PrintFunction(), new Fields());
        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage: WordCountTrident 0(storm-local)|1(storm-cluster) 127.0.0.1:6379,127.0.0.1:6380");
            System.exit(1);
        }

        Integer flag = Integer.valueOf(args[0]);
        String redisHostPort = args[1];

        Config conf = new Config();
        conf.setMaxSpoutPending(5);
        if (flag == 0) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test_wordCounter_for_redis", conf, buildTopology(redisHostPort));
            Thread.sleep(60 * 1000);
            cluster.killTopology("test_wordCounter_for_redis");
            cluster.shutdown();
            System.exit(0);
        } else if(flag == 1) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology("test_wordCounter_for_redis", conf, buildTopology(redisHostPort));
        } else {
            System.out.println("Usage: WordCountTrident 0(storm-local)|1(storm-cluster) redis-host redis-port");
        }
    }

}
