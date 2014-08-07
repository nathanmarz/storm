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
package storm.kafka;

import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.utils.Utils;
import com.google.common.collect.ImmutableMap;
import kafka.api.OffsetRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.javaapi.producer.Producer;
import kafka.message.MessageAndOffset;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import storm.kafka.trident.GlobalPartitionInformation;

import java.util.List;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class KafkaUtilsTest {

    private KafkaTestBroker broker;
    private SimpleConsumer simpleConsumer;
    private KafkaConfig config;
    private BrokerHosts brokerHosts;

    @Before
    public void setup() {
        broker = new KafkaTestBroker();
        GlobalPartitionInformation globalPartitionInformation = new GlobalPartitionInformation();
        globalPartitionInformation.addPartition(0, Broker.fromString(broker.getBrokerConnectionString()));
        brokerHosts = new StaticHosts(globalPartitionInformation);
        config = new KafkaConfig(brokerHosts, "testTopic");
        simpleConsumer = new SimpleConsumer("localhost", broker.getPort(), 60000, 1024, "testClient");
    }

    @After
    public void shutdown() {
        simpleConsumer.close();
        broker.shutdown();
    }


    @Test(expected = FailedFetchException.class)
    public void topicDoesNotExist() throws Exception {
        KafkaUtils.fetchMessages(config, simpleConsumer, new Partition(Broker.fromString(broker.getBrokerConnectionString()), 0), 0);
    }

    @Test(expected = FailedFetchException.class)
    public void brokerIsDown() throws Exception {
        int port = broker.getPort();
        broker.shutdown();
        SimpleConsumer simpleConsumer = new SimpleConsumer("localhost", port, 100, 1024, "testClient");
        try {
            KafkaUtils.fetchMessages(config, simpleConsumer, new Partition(Broker.fromString(broker.getBrokerConnectionString()), 0), OffsetRequest.LatestTime());
        } finally {
            simpleConsumer.close();
        }
    }

    @Test
    public void fetchMessage() throws Exception {
        String value = "test";
        createTopicAndSendMessage(value);
        long offset = KafkaUtils.getOffset(simpleConsumer, config.topic, 0, OffsetRequest.LatestTime()) - 1;
        ByteBufferMessageSet messageAndOffsets = KafkaUtils.fetchMessages(config, simpleConsumer,
                new Partition(Broker.fromString(broker.getBrokerConnectionString()), 0), offset);
        String message = new String(Utils.toByteArray(messageAndOffsets.iterator().next().message().payload()));
        assertThat(message, is(equalTo(value)));
    }

    @Test(expected = FailedFetchException.class)
    public void fetchMessagesWithInvalidOffsetAndDefaultHandlingDisabled() throws Exception {
        config.useStartOffsetTimeIfOffsetOutOfRange = false;
        KafkaUtils.fetchMessages(config, simpleConsumer,
                new Partition(Broker.fromString(broker.getBrokerConnectionString()), 0), -99);
    }

    @Test
    public void fetchMessagesWithInvalidOffsetAndDefaultHandlingEnabled() throws Exception {
        config = new KafkaConfig(brokerHosts, "newTopic");
        String value = "test";
        createTopicAndSendMessage(value);
        ByteBufferMessageSet messageAndOffsets = KafkaUtils.fetchMessages(config, simpleConsumer,
                new Partition(Broker.fromString(broker.getBrokerConnectionString()), 0), -99);
        String message = new String(Utils.toByteArray(messageAndOffsets.iterator().next().message().payload()));
        assertThat(message, is(equalTo(value)));
    }

    @Test
    public void getOffsetFromConfigAndDontForceFromStart() {
        config.forceFromStart = false;
        config.startOffsetTime = OffsetRequest.EarliestTime();
        createTopicAndSendMessage();
        long latestOffset = KafkaUtils.getOffset(simpleConsumer, config.topic, 0, OffsetRequest.LatestTime());
        long offsetFromConfig = KafkaUtils.getOffset(simpleConsumer, config.topic, 0, config);
        assertThat(latestOffset, is(equalTo(offsetFromConfig)));
    }

    @Test
    public void getOffsetFromConfigAndFroceFromStart() {
        config.forceFromStart = true;
        config.startOffsetTime = OffsetRequest.EarliestTime();
        createTopicAndSendMessage();
        long earliestOffset = KafkaUtils.getOffset(simpleConsumer, config.topic, 0, OffsetRequest.EarliestTime());
        long offsetFromConfig = KafkaUtils.getOffset(simpleConsumer, config.topic, 0, config);
        assertThat(earliestOffset, is(equalTo(offsetFromConfig)));
    }

    @Test
    public void generateTuplesWithoutKeyAndKeyValueScheme() {
        config.scheme = new KeyValueSchemeAsMultiScheme(new StringKeyValueScheme());
        runGetValueOnlyTuplesTest();
    }

    @Test
    public void generateTuplesWithKeyAndKeyValueScheme() {
        config.scheme = new KeyValueSchemeAsMultiScheme(new StringKeyValueScheme());
        String value = "value";
        String key = "key";
        createTopicAndSendMessage(key, value);
        ByteBufferMessageSet messageAndOffsets = getLastMessage();
        for (MessageAndOffset msg : messageAndOffsets) {
            Iterable<List<Object>> lists = KafkaUtils.generateTuples(config, msg.message());
            assertEquals(ImmutableMap.of(key, value), lists.iterator().next().get(0));
        }
    }

    @Test
    public void generateTupelsWithValueScheme() {
        config.scheme = new SchemeAsMultiScheme(new StringScheme());
        runGetValueOnlyTuplesTest();
    }

    @Test
    public void generateTuplesWithValueSchemeAndKeyValueMessage() {
        config.scheme = new SchemeAsMultiScheme(new StringScheme());
        String value = "value";
        String key = "key";
        createTopicAndSendMessage(key, value);
        ByteBufferMessageSet messageAndOffsets = getLastMessage();
        for (MessageAndOffset msg : messageAndOffsets) {
            Iterable<List<Object>> lists = KafkaUtils.generateTuples(config, msg.message());
            assertEquals(value, lists.iterator().next().get(0));
        }
    }

    private ByteBufferMessageSet getLastMessage() {
        long offsetOfLastMessage = KafkaUtils.getOffset(simpleConsumer, config.topic, 0, OffsetRequest.LatestTime()) - 1;
        return KafkaUtils.fetchMessages(config, simpleConsumer, new Partition(Broker.fromString(broker.getBrokerConnectionString()), 0), offsetOfLastMessage);
    }

    private void runGetValueOnlyTuplesTest() {
        String value = "value";
        createTopicAndSendMessage(null, value);
        ByteBufferMessageSet messageAndOffsets = getLastMessage();
        for (MessageAndOffset msg : messageAndOffsets) {
            Iterable<List<Object>> lists = KafkaUtils.generateTuples(config, msg.message());
            assertEquals(value, lists.iterator().next().get(0));
        }
    }


    private void createTopicAndSendMessage() {
        createTopicAndSendMessage(null, "someValue");
    }

    private void createTopicAndSendMessage(String value) {
        createTopicAndSendMessage(null, value);
    }

    private void createTopicAndSendMessage(String key, String value) {
        Properties p = new Properties();
        p.setProperty("metadata.broker.list", broker.getBrokerConnectionString());
        p.setProperty("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig producerConfig = new ProducerConfig(p);
        Producer<String, String> producer = new Producer<String, String>(producerConfig);
        producer.send(new KeyedMessage<String, String>(config.topic, key, value));
    }


    @Test
    public void assignOnePartitionPerTask() {
        runPartitionToTaskMappingTest(16, 1);
    }

    @Test
    public void assignTwoPartitionsPerTask() {
        runPartitionToTaskMappingTest(16, 2);
    }

    @Test
    public void assignAllPartitionsToOneTask() {
        runPartitionToTaskMappingTest(32, 32);
    }


    public void runPartitionToTaskMappingTest(int numPartitions, int partitionsPerTask) {
        GlobalPartitionInformation globalPartitionInformation = TestUtils.buildPartitionInfo(numPartitions);
        int numTasks = numPartitions / partitionsPerTask;
        for (int i = 0 ; i < numTasks ; i++) {
            assertEquals(partitionsPerTask, KafkaUtils.calculatePartitionsForTask(globalPartitionInformation, numTasks, i).size());
        }
    }

    @Test
    public void moreTasksThanPartitions() {
        GlobalPartitionInformation globalPartitionInformation = TestUtils.buildPartitionInfo(1);
        int numTasks = 2;
        assertEquals(1, KafkaUtils.calculatePartitionsForTask(globalPartitionInformation, numTasks, 0).size());
        assertEquals(0, KafkaUtils.calculatePartitionsForTask(globalPartitionInformation, numTasks, 1).size());
    }

    @Test (expected = IllegalArgumentException.class )
    public void assignInvalidTask() {
        GlobalPartitionInformation globalPartitionInformation = new GlobalPartitionInformation();
        KafkaUtils.calculatePartitionsForTask(globalPartitionInformation, 1, 1);
    }
}
