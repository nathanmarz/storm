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
package org.apache.storm.kafka.bolt.mapper;

import org.apache.storm.tuple.Tuple;

public class FieldNameBasedTupleToKafkaMapper<K,V> implements TupleToKafkaMapper<K, V> {

    public static final String BOLT_KEY = "key";
    public static final String BOLT_MESSAGE = "message";
    public String boltKeyField;
    public String boltMessageField;

    public FieldNameBasedTupleToKafkaMapper() {
        this(BOLT_KEY, BOLT_MESSAGE);
    }

    public FieldNameBasedTupleToKafkaMapper(String boltKeyField, String boltMessageField) {
        this.boltKeyField = boltKeyField;
        this.boltMessageField = boltMessageField;
    }

    @Override
    public K getKeyFromTuple(Tuple tuple) {
        //for backward compatibility, we return null when key is not present.
        return tuple.contains(boltKeyField) ? (K) tuple.getValueByField(boltKeyField) : null;
    }

    @Override
    public V getMessageFromTuple(Tuple tuple) {
        return (V) tuple.getValueByField(boltMessageField);
    }
}
