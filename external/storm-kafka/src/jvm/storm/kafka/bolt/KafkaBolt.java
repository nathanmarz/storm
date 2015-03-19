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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.bolt.mapper.TupleToKafkaMapper;
import storm.kafka.bolt.selector.DefaultTopicSelector;
import storm.kafka.bolt.selector.KafkaTopicSelector;

import java.util.Map;
import java.util.Properties;


/**
 * Bolt implementation that can send Tuple data to Kafka
 * <p/>
 * It expects the producer configuration and topic in storm config under
 * <p/>
 * 'kafka.broker.properties' and 'topic'
 * <p/>
 * respectively.
 */
public class KafkaBolt<K, V> extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaBolt.class);

    public static final String TOPIC = "topic";
    public static final String KAFKA_BROKER_PROPERTIES = "kafka.broker.properties";

    private Producer<K, V> producer;
    private OutputCollector collector;
    private TupleToKafkaMapper<K,V> mapper;
    private KafkaTopicSelector topicSelector;

    public KafkaBolt<K,V> withTupleToKafkaMapper(TupleToKafkaMapper<K,V> mapper) {
        this.mapper = mapper;
        return this;
    }

    public KafkaBolt<K,V> withTopicSelector(KafkaTopicSelector selector) {
        this.topicSelector = selector;
        return this;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        //for backward compatibility.
        if(mapper == null) {
            this.mapper = new FieldNameBasedTupleToKafkaMapper<K,V>();
        }

        //for backward compatibility.
        if(topicSelector == null) {
            this.topicSelector = new DefaultTopicSelector((String) stormConf.get(TOPIC));
        }

        Map configMap = (Map) stormConf.get(KAFKA_BROKER_PROPERTIES);
        Properties properties = new Properties();
        properties.putAll(configMap);
        ProducerConfig config = new ProducerConfig(properties);
        producer = new Producer<K, V>(config);
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        K key = null;
        V message = null;
        String topic = null;
        try {
            key = mapper.getKeyFromTuple(input);
            message = mapper.getMessageFromTuple(input);
            topic = topicSelector.getTopic(input);
            if(topic != null ) {
                producer.send(new KeyedMessage<K, V>(topic, key, message));
            } else {
                LOG.warn("skipping key = " + key + ", topic selector returned null.");
            }
            collector.ack(input);
        } catch (Exception ex) {
            collector.reportError(ex);
            collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
