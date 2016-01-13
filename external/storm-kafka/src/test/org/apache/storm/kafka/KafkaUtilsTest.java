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
package org.apache.storm.kafka;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.api.OffsetRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.kafka.trident.GlobalPartitionInformation;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.utils.Utils;

import com.google.common.collect.ImmutableMap;
public class KafkaUtilsTest {
    private String TEST_TOPIC = "testTopic";
    private static final Logger LOG = LoggerFactory.getLogger(KafkaUtilsTest.class);
    private KafkaTestBroker broker;
    private SimpleConsumer simpleConsumer;
    private KafkaConfig config;
    private BrokerHosts brokerHosts;

    @Before
    public void setup() {
        broker = new KafkaTestBroker();
        GlobalPartitionInformation globalPartitionInformation = new GlobalPartitionInformation(TEST_TOPIC);
        globalPartitionInformation.addPartition(0, Broker.fromString(broker.getBrokerConnectionString()));
        brokerHosts = new StaticHosts(globalPartitionInformation);
        config = new KafkaConfig(brokerHosts, TEST_TOPIC);
        simpleConsumer = new SimpleConsumer("localhost", broker.getPort(), 60000, 1024, "testClient");
    }

    @After
    public void shutdown() {
        simpleConsumer.close();
        broker.shutdown();
    }


    @Test(expected = FailedFetchException.class)
    public void topicDoesNotExist() throws Exception {
        KafkaUtils.fetchMessages(config, simpleConsumer, new Partition(Broker.fromString(broker.getBrokerConnectionString()), TEST_TOPIC, 0), 0);
    }

    @Test(expected = FailedFetchException.class)
    public void brokerIsDown() throws Exception {
        int port = broker.getPort();
        broker.shutdown();
        SimpleConsumer simpleConsumer = new SimpleConsumer("localhost", port, 100, 1024, "testClient");
        try {
            KafkaUtils.fetchMessages(config, simpleConsumer, new Partition(Broker.fromString(broker.getBrokerConnectionString()), TEST_TOPIC, 0), OffsetRequest.LatestTime());
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
                new Partition(Broker.fromString(broker.getBrokerConnectionString()), TEST_TOPIC, 0), offset);
        String message = new String(Utils.toByteArray(messageAndOffsets.iterator().next().message().payload()));
        assertThat(message, is(equalTo(value)));
    }

    @Test(expected = FailedFetchException.class)
    public void fetchMessagesWithInvalidOffsetAndDefaultHandlingDisabled() throws Exception {
        config.useStartOffsetTimeIfOffsetOutOfRange = false;
        KafkaUtils.fetchMessages(config, simpleConsumer,
                new Partition(Broker.fromString(broker.getBrokerConnectionString()), TEST_TOPIC, 0), -99);
    }

    @Test(expected = TopicOffsetOutOfRangeException.class)
    public void fetchMessagesWithInvalidOffsetAndDefaultHandlingEnabled() throws Exception {
        config = new KafkaConfig(brokerHosts, "newTopic");
        String value = "test";
        createTopicAndSendMessage(value);
        KafkaUtils.fetchMessages(config, simpleConsumer,
                new Partition(Broker.fromString(broker.getBrokerConnectionString()), "newTopic", 0), -99);
    }

    @Test
    public void getOffsetFromConfigAndDontForceFromStart() {
        config.ignoreZkOffsets = false;
        config.startOffsetTime = OffsetRequest.EarliestTime();
        createTopicAndSendMessage();
        long latestOffset = KafkaUtils.getOffset(simpleConsumer, config.topic, 0, OffsetRequest.EarliestTime());
        long offsetFromConfig = KafkaUtils.getOffset(simpleConsumer, config.topic, 0, config);
        assertThat(latestOffset, is(equalTo(offsetFromConfig)));
    }

    @Test
    public void getOffsetFromConfigAndFroceFromStart() {
        config.ignoreZkOffsets = true;
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
        config.useStartOffsetTimeIfOffsetOutOfRange = false;
        String value = "value";
        String key = "key";
        createTopicAndSendMessage(key, value);
        ByteBufferMessageSet messageAndOffsets = getLastMessage();
        for (MessageAndOffset msg : messageAndOffsets) {
            Iterable<List<Object>> lists = KafkaUtils.generateTuples(config, msg.message(), config.topic);
            assertEquals(ImmutableMap.of(key, value), lists.iterator().next().get(0));
        }
    }

    @Test
    public void generateTupelsWithValueScheme() {
        config.scheme = new SchemeAsMultiScheme(new StringScheme());
        runGetValueOnlyTuplesTest();
    }

    @Test
    public void generateTuplesWithValueAndStringMultiSchemeWithTopic() {
        config.scheme = new StringMultiSchemeWithTopic();
        String value = "value";
        createTopicAndSendMessage(value);
        ByteBufferMessageSet messageAndOffsets = getLastMessage();
        for (MessageAndOffset msg : messageAndOffsets) {
            Iterable<List<Object>> lists = KafkaUtils.generateTuples(config, msg.message(), config.topic);
            List<Object> list = lists.iterator().next();
            assertEquals(value, list.get(0));
            assertEquals(config.topic, list.get(1));
        }
    }

    @Test
    public void generateTuplesWithValueSchemeAndKeyValueMessage() {
        config.scheme = new SchemeAsMultiScheme(new StringScheme());
        String value = "value";
        String key = "key";
        createTopicAndSendMessage(key, value);
        ByteBufferMessageSet messageAndOffsets = getLastMessage();
        for (MessageAndOffset msg : messageAndOffsets) {
            Iterable<List<Object>> lists = KafkaUtils.generateTuples(config, msg.message(), config.topic);
            assertEquals(value, lists.iterator().next().get(0));
        }
    }
    
    @Test
    public void generateTuplesWithMessageAndMetadataScheme() {
        String value = "value";
        Partition mockPartition = Mockito.mock(Partition.class);
        mockPartition.partition = 0;
        long offset = 0L;
        
        MessageMetadataSchemeAsMultiScheme scheme = new MessageMetadataSchemeAsMultiScheme(new StringMessageAndMetadataScheme());
        
        createTopicAndSendMessage(null, value);
        ByteBufferMessageSet messageAndOffsets = getLastMessage();
        for (MessageAndOffset msg : messageAndOffsets) {
            Iterable<List<Object>> lists = KafkaUtils.generateTuples(scheme, msg.message(), mockPartition, offset);
            List<Object> values = lists.iterator().next(); 
            assertEquals("Message is incorrect", value, values.get(0));
            assertEquals("Partition is incorrect", mockPartition.partition, values.get(1));
            assertEquals("Offset is incorrect", offset, values.get(2));
        }
    }

    private ByteBufferMessageSet getLastMessage() {
        long offsetOfLastMessage = KafkaUtils.getOffset(simpleConsumer, config.topic, 0, OffsetRequest.LatestTime()) - 1;
        return KafkaUtils.fetchMessages(config, simpleConsumer, new Partition(Broker.fromString(broker.getBrokerConnectionString()), TEST_TOPIC, 0), offsetOfLastMessage);
    }

    private void runGetValueOnlyTuplesTest() {
        String value = "value";
        
        createTopicAndSendMessage(null, value);
        ByteBufferMessageSet messageAndOffsets = getLastMessage();
        for (MessageAndOffset msg : messageAndOffsets) {
            Iterable<List<Object>> lists = KafkaUtils.generateTuples(config, msg.message(), config.topic);
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
        p.put("acks", "1");
        p.put("bootstrap.servers", broker.getBrokerConnectionString());
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("metadata.fetch.timeout.ms", 1000);
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(p);
        try {
            producer.send(new ProducerRecord<String, String>(config.topic, key, value)).get();
        } catch (Exception e) {
            Assert.fail(e.getMessage());
            LOG.error("Failed to do synchronous sending due to " + e, e);
        } finally {
            producer.close();
        }
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
        List<GlobalPartitionInformation> partitions = new ArrayList<GlobalPartitionInformation>();
        partitions.add(globalPartitionInformation);
        int numTasks = numPartitions / partitionsPerTask;
        for (int i = 0 ; i < numTasks ; i++) {
            assertEquals(partitionsPerTask, KafkaUtils.calculatePartitionsForTask(partitions, numTasks, i).size());
        }
    }

    @Test
    public void moreTasksThanPartitions() {
        GlobalPartitionInformation globalPartitionInformation = TestUtils.buildPartitionInfo(1);
        List<GlobalPartitionInformation> partitions = new ArrayList<GlobalPartitionInformation>();
        partitions.add(globalPartitionInformation);
        int numTasks = 2;
        assertEquals(1, KafkaUtils.calculatePartitionsForTask(partitions, numTasks, 0).size());
        assertEquals(0, KafkaUtils.calculatePartitionsForTask(partitions, numTasks, 1).size());
    }

    @Test (expected = IllegalArgumentException.class )
    public void assignInvalidTask() {
        GlobalPartitionInformation globalPartitionInformation = new GlobalPartitionInformation(TEST_TOPIC);
        List<GlobalPartitionInformation> partitions = new ArrayList<GlobalPartitionInformation>();
        partitions.add(globalPartitionInformation);
        KafkaUtils.calculatePartitionsForTask(partitions, 1, 1);
    }
}
