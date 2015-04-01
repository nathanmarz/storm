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
package org.apache.storm.redis.trident.state;

import backtype.storm.tuple.Values;
import com.google.common.collect.Lists;
import org.apache.storm.redis.common.mapper.TupleMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;

import java.util.List;

public class RedisStateQuerier extends BaseQueryFunction<RedisState, String> {
    private static final Logger logger = LoggerFactory.getLogger(RedisState.class);

    private final String redisKeyPrefix;
    private final TupleMapper tupleMapper;

    public RedisStateQuerier(String redisKeyPrefix, TupleMapper tupleMapper) {
        this.redisKeyPrefix = redisKeyPrefix;
        this.tupleMapper = tupleMapper;
    }

    @Override
    public List<String> batchRetrieve(RedisState redisState, List<TridentTuple> inputs) {
        List<String> keys = Lists.newArrayList();
        for (TridentTuple input : inputs) {
            String key = this.tupleMapper.getKeyFromTuple(input);
            if (redisKeyPrefix != null && redisKeyPrefix.length() > 0) {
                key = redisKeyPrefix + key;
            }
            keys.add(key);
        }

        Jedis jedis = null;
        try {
            jedis = redisState.getJedis();
            return jedis.mget(keys.toArray(new String[keys.size()]));
        } finally {
            if (jedis != null) {
                redisState.returnJedis(jedis);
            }
        }
    }

    @Override
    public void execute(TridentTuple tuple, String s, TridentCollector collector) {
        String key = this.tupleMapper.getKeyFromTuple(tuple);
        collector.emit(new Values(key, s));
    }
}