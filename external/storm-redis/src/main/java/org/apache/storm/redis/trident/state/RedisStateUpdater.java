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

import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
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

    private final RedisStoreMapper storeMapper;
    private final int expireIntervalSec;

    public RedisStateUpdater(RedisStoreMapper storeMapper, int expireIntervalSec) {
        this.storeMapper = storeMapper;
        assertDataType(storeMapper.getDataTypeDescription());

        if (expireIntervalSec > 0) {
            this.expireIntervalSec = expireIntervalSec;
        } else {
            this.expireIntervalSec = 0;
        }
    }

    @Override
    public void updateState(RedisState redisState, List<TridentTuple> inputs,
                            TridentCollector collector) {
        Jedis jedis = null;
        try {
            jedis = redisState.getJedis();
            for (TridentTuple input : inputs) {
                String key = storeMapper.getKeyFromTuple(input);
                String value = storeMapper.getValueFromTuple(input);

                logger.debug("update key[" + key + "] redisKey[" + key+ "] value[" + value + "]");

                if (this.expireIntervalSec > 0) {
                    jedis.setex(key, expireIntervalSec, value);
                } else {
                    jedis.set(key, value);
                }
            }
        } finally {
            if (jedis != null) {
                redisState.returnJedis(jedis);
            }
        }
    }

    private void assertDataType(RedisDataTypeDescription storeMapper) {
        if (storeMapper.getDataType() != RedisDataTypeDescription.RedisDataType.STRING) {
            throw new IllegalArgumentException("State should be STRING type");
        }
    }
}
