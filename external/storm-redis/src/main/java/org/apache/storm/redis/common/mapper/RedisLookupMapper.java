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
package org.apache.storm.redis.common.mapper;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.ITuple;
import backtype.storm.tuple.Values;

import java.util.List;

public interface RedisLookupMapper extends TupleMapper, RedisMapper {
    /**
     * Converts return value from Redis to a list of storm values that can be emitted.
     * @param input the input tuple.
     * @param value Redis query response value. Can be String, Boolean, Long regarding of data type.
     * @return a List of storm values that can be emitted. Each item in list is emitted as an output tuple.
     */
    public List<Values> toTuple(ITuple input, Object value);

    /**
     * declare what are the fields that this code will output.
     * @param declarer
     */
    void declareOutputFields(OutputFieldsDeclarer declarer);
}
