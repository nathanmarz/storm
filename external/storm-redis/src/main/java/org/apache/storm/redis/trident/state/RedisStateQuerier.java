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
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * BaseQueryFunction implementation for single Redis environment.
 *
 * @see AbstractRedisStateQuerier
 */
public class RedisStateQuerier extends AbstractRedisStateQuerier<RedisState> {
    /**
     * Constructor
     *
     * @param lookupMapper mapper for querying
     */
    public RedisStateQuerier(RedisLookupMapper lookupMapper) {
        super(lookupMapper);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<String> retrieveValuesFromRedis(RedisState state, List<String> keys) {
        Jedis jedis = null;
        try {
            jedis = state.getJedis();
            List<String> redisVals;

            String[] keysForRedis = keys.toArray(new String[keys.size()]);
            switch (dataType) {
            case STRING:
                redisVals = jedis.mget(keysForRedis);
                break;
            case HASH:
                redisVals = jedis.hmget(additionalKey, keysForRedis);
                break;
            default:
                throw new IllegalArgumentException("Cannot process such data type: " + dataType);
            }

            return redisVals;
        } finally {
            if (jedis != null) {
                state.returnJedis(jedis);
            }
        }
    }
}