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
import org.apache.storm.redis.trident.mapper.TridentTupleMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;

import java.util.List;

/**
 * Created by judasheng on 14-12-12.
 */
public class RedisClusterStateQuerier extends BaseQueryFunction<RedisClusterState, String> {
    private static final Logger logger = LoggerFactory.getLogger(RedisClusterState.class);

    private final String redisKeyPrefix;
    private final TridentTupleMapper tupleMapper;

    public RedisClusterStateQuerier(String redisKeyPrefix, TridentTupleMapper tupleMapper) {
        this.redisKeyPrefix = redisKeyPrefix;
        this.tupleMapper = tupleMapper;
    }

    @Override
    public List<String> batchRetrieve(RedisClusterState redisClusterState, List<TridentTuple> inputs) {
        List<String> ret = Lists.newArrayList();

        List<String> keys = Lists.newArrayList();

        JedisCluster jedisCluster = null;
        try {
            jedisCluster = redisClusterState.getJedisCluster();


            for (TridentTuple input : inputs) {
                String key = this.tupleMapper.getKeyFromTridentTuple(input);
                if (redisKeyPrefix != null && redisKeyPrefix.length() > 0) {
                    key = redisKeyPrefix + key;
                }
                String value = jedisCluster.get(key);
                ret.add(value);

                logger.debug("redis get key[" + key + "] count[" + value + "]");
            }
        } finally {
            if (jedisCluster != null) {
                redisClusterState.returnJedisCluster(jedisCluster);
            }
        }

        return ret;
    }

    @Override
    public void execute(TridentTuple tuple, String s, TridentCollector collector) {
        String key = this.tupleMapper.getKeyFromTridentTuple(tuple);
        collector.emit(new Values(key, s));
    }
}