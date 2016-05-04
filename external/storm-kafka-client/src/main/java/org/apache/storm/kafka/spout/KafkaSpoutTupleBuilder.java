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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Implementations of {@link KafkaSpoutTupleBuilder} contain the logic to build tuples from {@link ConsumerRecord}s.
 * Users must subclass this abstract class to provide their implementation. See also {@link KafkaSpoutTuplesBuilder}
 */
public abstract class KafkaSpoutTupleBuilder<K,V> implements Serializable {
    private List<String> topics;

    /**
     * @param topics list of topics that use this implementation to build tuples
     */
    public KafkaSpoutTupleBuilder(String... topics) {
        if (topics == null || topics.length == 0) {
            throw new IllegalArgumentException("Must specify at least one topic. It cannot be null or empty");
        }
        this.topics = Arrays.asList(topics);
    }

    /**
     * @return list of topics that use this implementation to build tuples
     */
    public List<String> getTopics() {
        return Collections.unmodifiableList(topics);
    }

    /**
     * Builds a list of tuples using the ConsumerRecord specified as parameter
     * @param consumerRecord whose contents are used to build tuples
     * @return list of tuples
     */
    public abstract List<Object> buildTuple(ConsumerRecord<K, V> consumerRecord);
}
