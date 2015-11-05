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

import backtype.storm.Config;
import backtype.storm.utils.Utils;
import kafka.api.OffsetRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import storm.kafka.bolt.KafkaBolt;
import storm.kafka.trident.GlobalPartitionInformation;

import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class TestUtils {

    public static final String TOPIC = "test";

    public static GlobalPartitionInformation buildPartitionInfo(int numPartitions) {
        return buildPartitionInfo(numPartitions, 9092);
    }

    public static List<GlobalPartitionInformation> buildPartitionInfoList(GlobalPartitionInformation partitionInformation) {
        List<GlobalPartitionInformation> map = new ArrayList<GlobalPartitionInformation>();
        map.add(partitionInformation);
        return map;
    }

    public static GlobalPartitionInformation buildPartitionInfo(int numPartitions, int brokerPort) {
        GlobalPartitionInformation globalPartitionInformation = new GlobalPartitionInformation(TOPIC);
        for (int i = 0; i < numPartitions; i++) {
            globalPartitionInformation.addPartition(i, Broker.fromString("broker-" + i + " :" + brokerPort));
        }
        return globalPartitionInformation;
    }

    public static SimpleConsumer getKafkaConsumer(KafkaTestBroker broker) {
        BrokerHosts brokerHosts = getBrokerHosts(broker);
        KafkaConfig kafkaConfig = new KafkaConfig(brokerHosts, TOPIC);
        SimpleConsumer simpleConsumer = new SimpleConsumer("localhost", broker.getPort(), 60000, 1024, "testClient");
        return simpleConsumer;
    }

    public static KafkaConfig getKafkaConfig(KafkaTestBroker broker) {
        BrokerHosts brokerHosts = getBrokerHosts(broker);
        KafkaConfig kafkaConfig = new KafkaConfig(brokerHosts, TOPIC);
        return kafkaConfig;
    }

    private static BrokerHosts getBrokerHosts(KafkaTestBroker broker) {
        GlobalPartitionInformation globalPartitionInformation = new GlobalPartitionInformation(TOPIC);
        globalPartitionInformation.addPartition(0, Broker.fromString(broker.getBrokerConnectionString()));
        return new StaticHosts(globalPartitionInformation);
    }

    public static Config getConfig(String brokerConnectionString) {
        Config config = new Config();
        Properties props = new Properties();
        props.put("metadata.broker.list", brokerConnectionString);
        props.put("request.required.acks", "1");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        config.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);
        config.put(KafkaBolt.TOPIC, TOPIC);

        return config;
    }

    public static boolean verifyMessage(String key, String message, KafkaTestBroker broker, SimpleConsumer simpleConsumer) {
        long lastMessageOffset = KafkaUtils.getOffset(simpleConsumer, TestUtils.TOPIC, 0, OffsetRequest.LatestTime()) - 1;
        ByteBufferMessageSet messageAndOffsets = KafkaUtils.fetchMessages(TestUtils.getKafkaConfig(broker), simpleConsumer,
                new Partition(Broker.fromString(broker.getBrokerConnectionString()),TestUtils.TOPIC, 0), lastMessageOffset);
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
}
