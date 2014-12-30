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
package org.apache.storm.redis.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.util.config.JedisClusterConfig;
import org.apache.storm.redis.util.config.JedisPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

public class PersistentWordCount {
    private static final String WORD_SPOUT = "WORD_SPOUT";
    private static final String COUNT_BOLT = "COUNT_BOLT";
    private static final String REDIS_BOLT = "REDIS_BOLT";

    private static final String TEST_REDIS_HOST = "127.0.0.1";
    private static final int TEST_REDIS_PORT = 6379;

    public static class StoreCountRedisBolt extends AbstractRedisBolt {
        private static final Logger LOG = LoggerFactory.getLogger(StoreCountRedisBolt.class);

        public StoreCountRedisBolt(JedisPoolConfig config) {
            super(config);
        }

        public StoreCountRedisBolt(JedisClusterConfig config) {
            super(config);
        }

        @Override
        public void execute(Tuple input) {
            String word = input.getStringByField("word");
            int count = input.getIntegerByField("count");

            JedisCommands commands = null;
            try {
                commands = getInstance();
                commands.incrBy(word, count);
            } catch (JedisConnectionException e) {
                throw new RuntimeException("Unfortunately, this test requires redis-server running", e);
            } catch (JedisException e) {
                LOG.error("Exception occurred from Jedis/Redis", e);
            } finally {
                returnInstance(commands);
                this.collector.ack(input);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }
    }

    public static void main(String[] args) throws Exception {
        Config config = new Config();

        String host = TEST_REDIS_HOST;
        int port = TEST_REDIS_PORT;

        if (args.length > 2) {
            host = args[0];
            port = Integer.parseInt(args[1]);
        }

        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
                .setHost(host).setPort(port).build();

        WordSpout spout = new WordSpout();
        WordCounter bolt = new WordCounter();
        StoreCountRedisBolt redisBolt = new StoreCountRedisBolt(poolConfig);

        // wordSpout ==> countBolt ==> RedisBolt
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(WORD_SPOUT, spout, 1);
        builder.setBolt(COUNT_BOLT, bolt, 1).shuffleGrouping(WORD_SPOUT);
        builder.setBolt(REDIS_BOLT, redisBolt, 1).fieldsGrouping(COUNT_BOLT, new Fields("word"));

        if (args.length == 2) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", config, builder.createTopology());
            Thread.sleep(30000);
            cluster.killTopology("test");
            cluster.shutdown();
            System.exit(0);
        } else if (args.length == 3) {
            StormSubmitter.submitTopology(args[2], config, builder.createTopology());
        } else {
            System.out.println("Usage: PersistentWordCount <redis host> <redis port> (topology name)");
        }
    }
}
