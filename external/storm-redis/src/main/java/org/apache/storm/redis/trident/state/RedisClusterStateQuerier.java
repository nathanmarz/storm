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

import org.apache.storm.redis.common.mapper.RedisLookupMapper;
import redis.clients.jedis.JedisCluster;

import java.util.ArrayList;
import java.util.List;

/**
 * BaseQueryFunction implementation for Redis Cluster environment.
 *
 * @see AbstractRedisStateQuerier
 */
public class RedisClusterStateQuerier extends AbstractRedisStateQuerier<RedisClusterState> {
    /**
     * Constructor
     *
     * @param lookupMapper mapper for querying
     */
    public RedisClusterStateQuerier(RedisLookupMapper lookupMapper) {
        super(lookupMapper);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<String> retrieveValuesFromRedis(RedisClusterState state, List<String> keys) {
        JedisCluster jedisCluster = null;
        try {
            jedisCluster = state.getJedisCluster();
            List<String> redisVals = new ArrayList<String>();

            for (String key : keys) {
                switch (dataType) {
                case STRING:
                    redisVals.add(jedisCluster.get(key));
                    break;
                case HASH:
                    redisVals.add(jedisCluster.hget(additionalKey, key));
                    break;
                default:
                    throw new IllegalArgumentException("Cannot process such data type: " + dataType);
                }
            }

            return redisVals;
        } finally {
            if (jedisCluster != null) {
                state.returnJedisCluster(jedisCluster);
            }
        }
    }
}