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
package storm.kafka.bolt;

import backtype.storm.Config;
import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import kafka.api.OffsetRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import storm.kafka.*;
import storm.kafka.trident.GlobalPartitionInformation;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;

public class KafkaBoltTest {

    private static final String TEST_TOPIC = "test-topic";
    private KafkaTestBroker broker;
    private KafkaBolt bolt;
    private Config config = new Config();
    private KafkaConfig kafkaConfig;
    private SimpleConsumer simpleConsumer;

    @Mock
    private IOutputCollector collector;

    @Before
    public void initMocks() {
        MockitoAnnotations.initMocks(this);
        broker = new KafkaTestBroker();
        setupKafkaConsumer();
        config.put(KafkaBolt.TOPIC, TEST_TOPIC);
        bolt = generateStringSerializerBolt();
    }

    @After
    public void shutdown() {
        simpleConsumer.close();
        broker.shutdown();
    }


    private void setupKafkaConsumer() {
        GlobalPartitionInformation globalPartitionInformation = new GlobalPartitionInformation();
        globalPartitionInformation.addPartition(0, Broker.fromString(broker.getBrokerConnectionString()));
        BrokerHosts brokerHosts = new StaticHosts(globalPartitionInformation);
        kafkaConfig = new KafkaConfig(brokerHosts, TEST_TOPIC);
        simpleConsumer = new SimpleConsumer("localhost", broker.getPort(), 60000, 1024, "testClient");
    }

    @Test
    public void executeWithKey() throws Exception {
        String message = "value-123";
        String key = "key-123";
        Tuple tuple = generateTestTuple(key, message);
        bolt.execute(tuple);
        verify(collector).ack(tuple);
        verifyMessage(key, message);
    }

    @Test
    public void executeWithByteArrayKeyAndMessage() {
        bolt = generateDefaultSerializerBolt();
        String keyString = "test-key";
        String messageString = "test-message";
        byte[] key = keyString.getBytes();
        byte[] message = messageString.getBytes();
        Tuple tuple = generateTestTuple(key, message);
        bolt.execute(tuple);
        verify(collector).ack(tuple);
        verifyMessage(keyString, messageString);
    }

    private KafkaBolt generateStringSerializerBolt() {
        KafkaBolt bolt = new KafkaBolt();
        Properties props = new Properties();
        props.put("metadata.broker.list", broker.getBrokerConnectionString());
        props.put("request.required.acks", "1");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        config.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);
        bolt.prepare(config, null, new OutputCollector(collector));
        return bolt;
    }

    private KafkaBolt generateDefaultSerializerBolt() {
        KafkaBolt bolt = new KafkaBolt();
        Properties props = new Properties();
        props.put("metadata.broker.list", broker.getBrokerConnectionString());
        props.put("request.required.acks", "1");
        config.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);
        bolt.prepare(config, null, new OutputCollector(collector));
        return bolt;
    }

    @Test
    public void executeWithoutKey() throws Exception {
        String message = "value-234";
        Tuple tuple = generateTestTuple(message);
        bolt.execute(tuple);
        verify(collector).ack(tuple);
        verifyMessage(null, message);
    }


    @Test
    public void executeWithBrokerDown() throws Exception {
        broker.shutdown();
        String message = "value-234";
        Tuple tuple = generateTestTuple(message);
        bolt.execute(tuple);
        verify(collector).ack(tuple);
    }


    private boolean verifyMessage(String key, String message) {
        long lastMessageOffset = KafkaUtils.getOffset(simpleConsumer, kafkaConfig.topic, 0, OffsetRequest.LatestTime()) - 1;
        ByteBufferMessageSet messageAndOffsets = KafkaUtils.fetchMessages(kafkaConfig, simpleConsumer,
                new Partition(Broker.fromString(broker.getBrokerConnectionString()), 0), lastMessageOffset);
        MessageAndOffset messageAndOffset = messageAndOffsets.iterator().next();
        Message kafkaMessage = messageAndOffset.message();
        ByteBuffer messageKeyBuffer = kafkaMessage.key();
        String keyString = null;
        String messageString = new String(Utils.toByteArray(kafkaMessage.payload()));
        if (messageKeyBuffer != null) {
            keyString = new String(Utils.toByteArray(messageKeyBuffer));
        }
        assertEquals(key, keyString);
        assertEquals(message, messageString);
        return true;
    }

    private Tuple generateTestTuple(Object key, Object message) {
        TopologyBuilder builder = new TopologyBuilder();
        GeneralTopologyContext topologyContext = new GeneralTopologyContext(builder.createTopology(), new Config(), new HashMap(), new HashMap(), new HashMap(), "") {
            @Override
            public Fields getComponentOutputFields(String componentId, String streamId) {
                return new Fields("key", "message");
            }
        };
        return new TupleImpl(topologyContext, new Values(key, message), 1, "");
    }

    private Tuple generateTestTuple(Object message) {
        TopologyBuilder builder = new TopologyBuilder();
        GeneralTopologyContext topologyContext = new GeneralTopologyContext(builder.createTopology(), new Config(), new HashMap(), new HashMap(), new HashMap(), "") {
            @Override
            public Fields getComponentOutputFields(String componentId, String streamId) {
                return new Fields("message");
            }
        };
        return new TupleImpl(topologyContext, new Values(message), 1, "");
    }
}
