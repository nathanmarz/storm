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
package org.apache.storm.kafka.bolt;

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.task.IOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;
import org.apache.storm.utils.Utils;
import com.google.common.collect.ImmutableList;
import kafka.api.OffsetRequest;
import kafka.api.FetchRequest;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.*;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.apache.storm.kafka.*;
import org.apache.storm.kafka.trident.GlobalPartitionInformation;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

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
        bolt.cleanup();
    }

    private void setupKafkaConsumer() {
        GlobalPartitionInformation globalPartitionInformation = new GlobalPartitionInformation(TEST_TOPIC);
        globalPartitionInformation.addPartition(0, Broker.fromString(broker.getBrokerConnectionString()));
        BrokerHosts brokerHosts = new StaticHosts(globalPartitionInformation);
        kafkaConfig = new KafkaConfig(brokerHosts, TEST_TOPIC);
        simpleConsumer = new SimpleConsumer("localhost", broker.getPort(), 60000, 1024, "testClient");
    }

    @Test
    public void shouldAcknowledgeTickTuples() throws Exception {
        // Given
        Tuple tickTuple = mockTickTuple();

        // When
        bolt.execute(tickTuple);

        // Then
        verify(collector).ack(tickTuple);
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

    /* test synchronous sending */
    @Test
    public void executeWithByteArrayKeyAndMessageSync() {
        boolean async = false;
        boolean fireAndForget = false;
        bolt = generateDefaultSerializerBolt(async, fireAndForget, null);
        String keyString = "test-key";
        String messageString = "test-message";
        byte[] key = keyString.getBytes();
        byte[] message = messageString.getBytes();
        Tuple tuple = generateTestTuple(key, message);
        bolt.execute(tuple);
        verify(collector).ack(tuple);
        verifyMessage(keyString, messageString);
    }

    /* test asynchronous sending (default) */
    @Test
    public void executeWithByteArrayKeyAndMessageAsync() {
        boolean async = true;
        boolean fireAndForget = false;
        String keyString = "test-key";
        String messageString = "test-message";
        byte[] key = keyString.getBytes();
        byte[] message = messageString.getBytes();
        final Tuple tuple = generateTestTuple(key, message);

        final ByteBufferMessageSet mockMsg = mockSingleMessage(key, message);
        simpleConsumer.close();
        simpleConsumer = mockSimpleConsumer(mockMsg);
        KafkaProducer<?, ?> producer = mock(KafkaProducer.class);
        when(producer.send(any(ProducerRecord.class), any(Callback.class))).thenAnswer(new Answer<Future>() {
            @Override
            public Future answer(InvocationOnMock invocationOnMock) throws Throwable {
                Callback cb = (Callback) invocationOnMock.getArguments()[1];
                cb.onCompletion(null, null);
                return mock(Future.class);
            }
        });
        bolt = generateDefaultSerializerBolt(async, fireAndForget, producer);
        bolt.execute(tuple);
        verify(collector).ack(tuple);
        verifyMessage(keyString, messageString);
    }

    /* test with fireAndForget option enabled */
    @Test
    public void executeWithByteArrayKeyAndMessageFire() {
        boolean async = true;
        boolean fireAndForget = true;
        bolt = generateDefaultSerializerBolt(async, fireAndForget, null);
        String keyString = "test-key";
        String messageString = "test-message";
        byte[] key = keyString.getBytes();
        byte[] message = messageString.getBytes();
        Tuple tuple = generateTestTuple(key, message);
        final ByteBufferMessageSet mockMsg = mockSingleMessage(key, message);
        simpleConsumer.close();
        simpleConsumer = mockSimpleConsumer(mockMsg);
        KafkaProducer<?, ?> producer = mock(KafkaProducer.class);
        // do not invoke the callback of send() in order to test whether the bolt handle the fireAndForget option
        // properly.
        doReturn(mock(Future.class)).when(producer).send(any(ProducerRecord.class), any(Callback.class));
        bolt.execute(tuple);
        verify(collector).ack(tuple);
        verifyMessage(keyString, messageString);
    }

    /* test bolt specified properties */
    @Test
    public void executeWithBoltSpecifiedProperties() {
        boolean async = false;
        boolean fireAndForget = false;
        bolt = defaultSerializerBoltWithSpecifiedProperties(async, fireAndForget);
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
        Properties props = new Properties();
        props.put("acks", "1");
        props.put("bootstrap.servers", broker.getBrokerConnectionString());
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("metadata.fetch.timeout.ms", 1000);
        KafkaBolt bolt = new KafkaBolt().withProducerProperties(props);
        bolt.prepare(config, null, new OutputCollector(collector));
        bolt.setAsync(false);
        return bolt;
    }

    private KafkaBolt generateDefaultSerializerBolt(boolean async, boolean fireAndForget,
                                                    KafkaProducer<?, ?> mockProducer) {
        Properties props = new Properties();
        props.put("acks", "1");
        props.put("bootstrap.servers", broker.getBrokerConnectionString());
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("metadata.fetch.timeout.ms", 1000);
        props.put("linger.ms", 0);
        KafkaBolt bolt = new KafkaBolt().withProducerProperties(props);
        bolt.prepare(config, null, new OutputCollector(collector));
        bolt.setAsync(async);
        bolt.setFireAndForget(fireAndForget);
        if (mockProducer != null) {
            Whitebox.setInternalState(bolt, "producer", mockProducer);
        }
        return bolt;
    }

    private KafkaBolt defaultSerializerBoltWithSpecifiedProperties(boolean async, boolean fireAndForget) {
        Properties props = new Properties();
        props.put("acks", "1");
        props.put("bootstrap.servers", broker.getBrokerConnectionString());
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("metadata.fetch.timeout.ms", 1000);
        props.put("linger.ms", 0);
        KafkaBolt bolt = new KafkaBolt().withProducerProperties(props);
        bolt.prepare(config, null, new OutputCollector(collector));
        bolt.setAsync(async);
        bolt.setFireAndForget(fireAndForget);
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
        verify(collector).fail(tuple);
    }

    private boolean verifyMessage(String key, String message) {
        long lastMessageOffset = KafkaUtils.getOffset(simpleConsumer, kafkaConfig.topic, 0, OffsetRequest.LatestTime()) - 1;
        ByteBufferMessageSet messageAndOffsets = KafkaUtils.fetchMessages(kafkaConfig, simpleConsumer,
                new Partition(Broker.fromString(broker.getBrokerConnectionString()),kafkaConfig.topic, 0), lastMessageOffset);
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

    private Tuple mockTickTuple() {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getSourceComponent()).thenReturn(Constants.SYSTEM_COMPONENT_ID);
        when(tuple.getSourceStreamId()).thenReturn(Constants.SYSTEM_TICK_STREAM_ID);
        // Sanity check
        assertTrue(TupleUtils.isTick(tuple));
        return tuple;
    }

    private static ByteBufferMessageSet mockSingleMessage(byte[] key, byte[] message) {
        ByteBufferMessageSet sets = mock(ByteBufferMessageSet.class);
        MessageAndOffset msg = mock(MessageAndOffset.class);
        final List<MessageAndOffset> msgs = ImmutableList.of(msg);
        doReturn(msgs.iterator()).when(sets).iterator();
        Message kafkaMessage = mock(Message.class);
        doReturn(ByteBuffer.wrap(key)).when(kafkaMessage).key();
        doReturn(ByteBuffer.wrap(message)).when(kafkaMessage).payload();
        doReturn(kafkaMessage).when(msg).message();
        return sets;
    }

    private static SimpleConsumer mockSimpleConsumer(ByteBufferMessageSet mockMsg) {
        SimpleConsumer simpleConsumer = mock(SimpleConsumer.class);
        FetchResponse resp = mock(FetchResponse.class);
        doReturn(resp).when(simpleConsumer).fetch(any(FetchRequest.class));
        OffsetResponse mockOffsetResponse = mock(OffsetResponse.class);
        doReturn(new long[] {}).when(mockOffsetResponse).offsets(anyString(), anyInt());
        doReturn(mockOffsetResponse).when(simpleConsumer).getOffsetsBefore(any(kafka.javaapi.OffsetRequest.class));
        doReturn(mockMsg).when(resp).messageSet(anyString(), anyInt());
        return simpleConsumer;
    }
}
