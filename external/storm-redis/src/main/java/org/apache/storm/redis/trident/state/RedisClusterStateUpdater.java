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

import org.apache.storm.redis.trident.mapper.TridentTupleMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.List;

public class RedisClusterStateUpdater extends BaseStateUpdater<RedisClusterState> {
    private static final Logger logger = LoggerFactory.getLogger(RedisClusterState.class);

    private static final long DEFAULT_EXPIRE_INTERVAL_MS = 86400000;

    private final String redisKeyPrefix;
    private final TridentTupleMapper tupleMapper;
    private final long expireIntervalMs;

    public RedisClusterStateUpdater(String redisKeyPrefix, TridentTupleMapper tupleMapper, long expireIntervalMs) {
        this.redisKeyPrefix = redisKeyPrefix;
        this.tupleMapper = tupleMapper;
        if (expireIntervalMs > 0) {
            this.expireIntervalMs = expireIntervalMs;
        } else {
            this.expireIntervalMs = DEFAULT_EXPIRE_INTERVAL_MS;
        }
    }

    @Override
    public void updateState(RedisClusterState redisClusterState, List<TridentTuple> inputs,
                            TridentCollector collector) {
        long expireAt = System.currentTimeMillis() + expireIntervalMs;

        JedisCluster jedisCluster = null;
        try {
            jedisCluster = redisClusterState.getJedisCluster();
            for (TridentTuple input : inputs) {
                String key = this.tupleMapper.getKeyFromTridentTuple(input);
                String redisKey = key;
                if (redisKeyPrefix != null && redisKeyPrefix.length() > 0) {
                    redisKey = redisKeyPrefix + redisKey;
                }
                String value = this.tupleMapper.getValueFromTridentTuple(input);

                logger.debug("update key[" + key + "] redisKey[" + redisKey + "] value[" + value + "]");

                jedisCluster.set(redisKey, value);
                jedisCluster.expireAt(redisKey, expireAt);
            }
        } finally {
            if (jedisCluster != null) {
                redisClusterState.returnJedisCluster(jedisCluster);
            }
        }
    }
}
