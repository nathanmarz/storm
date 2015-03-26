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

import org.apache.storm.redis.common.mapper.TupleMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.List;

public class RedisStateUpdater extends BaseStateUpdater<RedisState> {
    private static final Logger logger = LoggerFactory.getLogger(RedisState.class);

    private final String redisKeyPrefix;
    private final TupleMapper tupleMapper;
    private int expireIntervalSec = 0;

    public RedisStateUpdater(String redisKeyPrefix, TupleMapper tupleMapper) {
        this.redisKeyPrefix = redisKeyPrefix;
        this.tupleMapper = tupleMapper;
    }

    public RedisStateUpdater withExpire(int expireIntervalSec) {
        if (expireIntervalSec > 0) {
            this.expireIntervalSec = expireIntervalSec;
        } else {
            this.expireIntervalSec = 0;
        }

        return this;
    }

    @Override
    public void updateState(RedisState redisState, List<TridentTuple> inputs,
                            TridentCollector collector) {
        Jedis jedis = null;
        try {
            jedis = redisState.getJedis();
            for (TridentTuple input : inputs) {
                String key = this.tupleMapper.getKeyFromTuple(input);
                String redisKey = key;
                if (redisKeyPrefix != null && redisKeyPrefix.length() > 0) {
                    redisKey = redisKeyPrefix + redisKey;
                }
                String value = this.tupleMapper.getValueFromTuple(input);

                logger.debug("update key[" + key + "] redisKey[" + redisKey + "] value[" + value + "]");

                if (this.expireIntervalSec > 0) {
                    jedis.setex(redisKey, expireIntervalSec, value);
                } else {
                    jedis.set(redisKey, value);
                }
            }
        } finally {
            if (jedis != null) {
                redisState.returnJedis(jedis);
            }
        }
    }
}
