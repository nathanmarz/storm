/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * Contains some contributions under the Thrift Software License.
 * Please see doc/old-thrift-license.txt in the Thrift distribution for
 * details.
 */
package org.apache.storm.starter.trident;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.starter.spout.RandomSentenceSpout;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.testing.Split;

import java.util.Properties;

/**
 * A sample word count trident topology using transactional kafka spout that has the following components.
 * <ol>
 * <li> {@link KafkaBolt}
 * that receives random sentences from {@link RandomSentenceSpout} and
 * publishes the sentences to a kafka "test" topic.
 * </li>
 * <li> {@link TransactionalTridentKafkaSpout}
 * that consumes sentences from the "test" topic, splits it into words, aggregates
 * and stores the word count in a {@link MemoryMapState}.
 * </li>
 * <li> DRPC query
 * that returns the word counts by querying the trident state (MemoryMapState).
 * </li>
 * </ol>
 * <p>
 *     For more background read the <a href="https://storm.apache.org/documentation/Trident-tutorial.html">trident tutorial</a>,
 *     <a href="https://storm.apache.org/documentation/Trident-state">trident state</a> and
 *     <a href="https://github.com/apache/storm/tree/master/external/storm-kafka"> Storm Kafka </a>.
 * </p>
 */
public class TridentKafkaWordCount {

    private String zkUrl;
    private String brokerUrl;

    TridentKafkaWordCount(String zkUrl, String brokerUrl) {
        this.zkUrl = zkUrl;
        this.brokerUrl = brokerUrl;
    }

    /**
     * Creates a transactional kafka spout that consumes any new data published to "test" topic.
     * <p/>
     * For more info on transactional spouts
     * see "Transactional spouts" section in
     * <a href="https://storm.apache.org/documentation/Trident-state"> Trident state</a> doc.
     *
     * @return a transactional trident kafka spout.
     */
    private TransactionalTridentKafkaSpout createKafkaSpout() {
        ZkHosts hosts = new ZkHosts(zkUrl);
        TridentKafkaConfig config = new TridentKafkaConfig(hosts, "test");
        config.scheme = new SchemeAsMultiScheme(new StringScheme());

        // Consume new data from the topic
        config.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        return new TransactionalTridentKafkaSpout(config);
    }


    private Stream addDRPCStream(TridentTopology tridentTopology, TridentState state, LocalDRPC drpc) {
        return tridentTopology.newDRPCStream("words", drpc)
                .each(new Fields("args"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .stateQuery(state, new Fields("word"), new MapGet(), new Fields("count"))
                .each(new Fields("count"), new FilterNull())
                .project(new Fields("word", "count"));
    }

    private TridentState addTridentState(TridentTopology tridentTopology) {
        return tridentTopology.newStream("spout1", createKafkaSpout()).parallelismHint(1)
                .each(new Fields("str"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .parallelismHint(1);
    }

    /**
     * Creates a trident topology that consumes sentences from the kafka "test" topic using a
     * {@link TransactionalTridentKafkaSpout} computes the word count and stores it in a {@link MemoryMapState}.
     * A DRPC stream is then created to query the word counts.
     * @param drpc
     * @return
     */
    public StormTopology buildConsumerTopology(LocalDRPC drpc) {
        TridentTopology tridentTopology = new TridentTopology();
        addDRPCStream(tridentTopology, addTridentState(tridentTopology), drpc);
        return tridentTopology.build();
    }

    /**
     * Return the consumer topology config.
     *
     * @return the topology config
     */
    public Config getConsumerConfig() {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        //  conf.setDebug(true);
        return conf;
    }

    /**
     * A topology that produces random sentences using {@link RandomSentenceSpout} and
     * publishes the sentences using a KafkaBolt to kafka "test" topic.
     *
     * @return the storm topology
     */
    public StormTopology buildProducerTopology(Properties prop) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new RandomSentenceSpout(), 2);
        /**
         * The output field of the RandomSentenceSpout ("word") is provided as the boltMessageField
         * so that this gets written out as the message in the kafka topic.
         */
        KafkaBolt bolt = new KafkaBolt().withProducerProperties(prop)
                .withTopicSelector(new DefaultTopicSelector("test"))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("key", "word"));
        builder.setBolt("forwardToKafka", bolt, 1).shuffleGrouping("spout");
        return builder.createTopology();
    }

    /**
     * Returns the storm config for the topology that publishes sentences to kafka "test" topic using a kafka bolt.
     * The KAFKA_BROKER_PROPERTIES is needed for the KafkaBolt.
     *
     * @return the topology config
     */
    public Properties getProducerConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "storm-kafka-producer");
        return props;
    }

    /**
     * <p>
     * To run this topology ensure you have a kafka broker running.
     * </p>
     * Create a topic test with command line,
     * kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partition 1 --topic test
     */
    public static void main(String[] args) throws Exception {

        String zkUrl = "localhost:2181";        // the defaults.
        String brokerUrl = "localhost:9092";

        if (args.length > 2 || (args.length == 1 && args[0].matches("^-h|--help$"))) {
            System.out.println("Usage: TridentKafkaWordCount [kafka zookeeper url] [kafka broker url]");
            System.out.println("   E.g TridentKafkaWordCount [" + zkUrl + "]" + " [" + brokerUrl + "]");
            System.exit(1);
        } else if (args.length == 1) {
            zkUrl = args[0];
        } else if (args.length == 2) {
            zkUrl = args[0];
            brokerUrl = args[1];
        }

        System.out.println("Using Kafka zookeeper url: " + zkUrl + " broker url: " + brokerUrl);

        TridentKafkaWordCount wordCount = new TridentKafkaWordCount(zkUrl, brokerUrl);

        LocalDRPC drpc = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();

        // submit the consumer topology.
        cluster.submitTopology("wordCounter", wordCount.getConsumerConfig(), wordCount.buildConsumerTopology(drpc));

        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        // submit the producer topology.
        cluster.submitTopology("kafkaBolt", conf, wordCount.buildProducerTopology(wordCount.getProducerConfig()));

        // keep querying the word counts for a minute.
        for (int i = 0; i < 60; i++) {
            System.out.println("DRPC RESULT: " + drpc.execute("words", "the and apple snow jumped"));
            Thread.sleep(1000);
        }

        cluster.killTopology("kafkaBolt");
        cluster.killTopology("wordCounter");
        cluster.shutdown();
    }
}
