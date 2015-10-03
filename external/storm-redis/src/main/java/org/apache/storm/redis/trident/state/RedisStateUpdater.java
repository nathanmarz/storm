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
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.Map;

/**
 * BaseStateUpdater implementation for single Redis environment.
 *
 * @see AbstractRedisStateUpdater
 */
public class RedisStateUpdater extends AbstractRedisStateUpdater<RedisState> {
    /**
     * Constructor
     *
     * @param storeMapper mapper for storing
     */
    public RedisStateUpdater(RedisStoreMapper storeMapper) {
        super(storeMapper);
    }

    /**
     * Sets expire (time to live) if needed.
     *
     * @param expireIntervalSec time to live in seconds
     * @return RedisStateUpdater itself
     */
    public RedisStateUpdater withExpire(int expireIntervalSec) {
        setExpireInterval(expireIntervalSec);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void updateStatesToRedis(RedisState redisState, Map<String, String> keyToValue) {
        Jedis jedis = null;
        try {
            jedis = redisState.getJedis();
            Pipeline pipeline = jedis.pipelined();

            for (Map.Entry<String, String> kvEntry : keyToValue.entrySet()) {
                String key = kvEntry.getKey();
                String value = kvEntry.getValue();

                switch (dataType) {
                case STRING:
                    if (this.expireIntervalSec > 0) {
                        pipeline.setex(key, expireIntervalSec, value);
                    } else {
                        pipeline.set(key, value);
                    }
                    break;
                case HASH:
                    pipeline.hset(additionalKey, key, value);
                    break;
                default:
                    throw new IllegalArgumentException("Cannot process such data type: " + dataType);
                }
            }

            // send expire command for hash only once
            // it expires key itself entirely, so use it with caution
            if (dataType == RedisDataTypeDescription.RedisDataType.HASH &&
                    this.expireIntervalSec > 0) {
                pipeline.expire(additionalKey, expireIntervalSec);
            }

            pipeline.sync();
        } finally {
            if (jedis != null) {
                redisState.returnJedis(jedis);
            }
        }
    }

}
