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
import redis.clients.jedis.JedisCluster;

import java.util.Map;

/**
 * BaseStateUpdater implementation for Redis Cluster environment.
 *
 * @see AbstractRedisStateUpdater
 */
public class RedisClusterStateUpdater extends AbstractRedisStateUpdater<RedisClusterState> {
    /**
     * Constructor
     *
     * @param storeMapper mapper for storing
     */
    public RedisClusterStateUpdater(RedisStoreMapper storeMapper) {
        super(storeMapper);
    }

    /**
     * Sets expire (time to live) if needed.
     *
     * @param expireIntervalSec time to live in seconds
     * @return RedisClusterStateUpdater itself
     */
    public RedisClusterStateUpdater withExpire(int expireIntervalSec) {
        setExpireInterval(expireIntervalSec);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void updateStatesToRedis(RedisClusterState redisClusterState, Map<String, String> keyToValue) {
        JedisCluster jedisCluster = null;
        try {
            jedisCluster = redisClusterState.getJedisCluster();

            for (Map.Entry<String, String> kvEntry : keyToValue.entrySet()) {
                String key = kvEntry.getKey();
                String value = kvEntry.getValue();

                switch (dataType) {
                case STRING:
                    if (this.expireIntervalSec > 0) {
                        jedisCluster.setex(key, expireIntervalSec, value);
                    } else {
                        jedisCluster.set(key, value);
                    }
                    break;
                case HASH:
                    jedisCluster.hset(additionalKey, key, value);
                    break;
                default:
                    throw new IllegalArgumentException("Cannot process such data type: " + dataType);
                }
            }

            // send expire command for hash only once
            // it expires key itself entirely, so use it with caution
            if (dataType == RedisDataTypeDescription.RedisDataType.HASH &&
                    this.expireIntervalSec > 0) {
                jedisCluster.expire(additionalKey, expireIntervalSec);
            }
        } finally {
            if (jedisCluster != null) {
                redisClusterState.returnJedisCluster(jedisCluster);
            }
        }
    }
}
