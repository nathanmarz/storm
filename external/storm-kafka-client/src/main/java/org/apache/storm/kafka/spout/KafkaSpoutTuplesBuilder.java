/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.storm.kafka.spout;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link KafkaSpoutTuplesBuilder} wraps all the logic that builds tuples from {@link ConsumerRecord}s.
 * The logic is provided by the user by implementing the appropriate number of {@link KafkaSpoutTupleBuilder} instances
 */
public class KafkaSpoutTuplesBuilder<K,V> implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSpoutTuplesBuilder.class);

    private Map<String, KafkaSpoutTupleBuilder<K, V>> topicToTupleBuilders;

    private KafkaSpoutTuplesBuilder(Builder<K,V> builder) {
        this.topicToTupleBuilders = builder.topicToTupleBuilders;
        LOG.debug("Instantiated {}", this);
    }

    public static class Builder<K,V> {
        private List<KafkaSpoutTupleBuilder<K, V>> tupleBuilders;
        private Map<String, KafkaSpoutTupleBuilder<K, V>> topicToTupleBuilders;

        @SafeVarargs
        public Builder(KafkaSpoutTupleBuilder<K,V>... tupleBuilders) {
            if (tupleBuilders == null || tupleBuilders.length == 0) {
                throw new IllegalArgumentException("Must specify at last one tuple builder per topic declared in KafkaSpoutStreams");
            }

            this.tupleBuilders = Arrays.asList(tupleBuilders);
            topicToTupleBuilders = new HashMap<>();
        }

        public KafkaSpoutTuplesBuilder<K,V> build() {
            for (KafkaSpoutTupleBuilder<K, V> tupleBuilder : tupleBuilders) {
                for (String topic : tupleBuilder.getTopics()) {
                    if (!topicToTupleBuilders.containsKey(topic)) {
                        topicToTupleBuilders.put(topic, tupleBuilder);
                    }
                }
            }
            return new KafkaSpoutTuplesBuilder<>(this);
        }
    }

    public List<Object>buildTuple(ConsumerRecord<K,V> consumerRecord) {
        final String topic = consumerRecord.topic();
        return topicToTupleBuilders.get(topic).buildTuple(consumerRecord);
    }

    @Override
    public String toString() {
        return "KafkaSpoutTuplesBuilder{" +
                "topicToTupleBuilders=" + topicToTupleBuilders +
                '}';
    }
}
