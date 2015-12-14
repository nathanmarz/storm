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
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.apache.storm.kafka.spout;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.List;

public class KafkaRecordTupleBuilder<K, V> implements KafkaSpoutTupleBuilder<K, V> {
    @Override
    public List<Object> buildTuple(final ConsumerRecord<K, V> consumerRecord, KafkaSpoutStreams kafkaSpoutStreams) {
        final Fields outputFields = kafkaSpoutStreams.getOutputFields(consumerRecord.topic());
        if (outputFields != null) {
            if (outputFields.size() == 3) {
                return new Values(consumerRecord.topic(),
                        consumerRecord.partition(),
                        consumerRecord.offset());
            } else if (outputFields.size() == 5) {
                return new Values(consumerRecord.topic(),
                        consumerRecord.partition(),
                        consumerRecord.offset(),
                        consumerRecord.key(),
                        consumerRecord.value());
            }
        }
        throw new RuntimeException("Failed to build tuple. " + consumerRecord + " " + kafkaSpoutStreams);
    }
}