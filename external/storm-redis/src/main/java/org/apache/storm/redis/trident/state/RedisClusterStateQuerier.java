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
import org.apache.storm.redis.common.mapper.RedisLookupMapper;
import org.apache.storm.redis.common.mapper.TupleMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;

import java.util.List;

public class RedisClusterStateQuerier extends BaseQueryFunction<RedisClusterState, List<Values>> {
    private static final Logger logger = LoggerFactory.getLogger(RedisClusterState.class);

    private final RedisLookupMapper lookupMapper;

    public RedisClusterStateQuerier(RedisLookupMapper lookupMapper) {
        this.lookupMapper = lookupMapper;
    }

    @Override
    public List<List<Values>> batchRetrieve(RedisClusterState redisClusterState, List<TridentTuple> inputs) {
        List<List<Values>> ret = Lists.newArrayList();

        JedisCluster jedisCluster = null;
        try {
            jedisCluster = redisClusterState.getJedisCluster();

            for (int i = 0 ; i < inputs.size() ; i++) {
                TridentTuple input = inputs.get(i);

                String key = lookupMapper.getKeyFromTuple(input);
                String value = jedisCluster.get(key);
                ret.add(lookupMapper.toTuple(input, value));
                logger.debug("redis get key[" + key + "] value [" + value + "]");
            }
        } finally {
            if (jedisCluster != null) {
                redisClusterState.returnJedisCluster(jedisCluster);
            }
        }

        return ret;
    }

    @Override
    public void execute(TridentTuple tuple, List<Values> values, TridentCollector collector) {
        for (Values value : values) {
            collector.emit(value);
        }
    }
}