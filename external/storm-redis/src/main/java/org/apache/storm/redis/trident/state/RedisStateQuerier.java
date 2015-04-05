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
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisLookupMapper;
import redis.clients.jedis.Jedis;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

public class RedisStateQuerier extends BaseQueryFunction<RedisState, List<Values>> {
    private final RedisLookupMapper lookupMapper;

    public RedisStateQuerier(RedisLookupMapper lookupMapper) {
        this.lookupMapper = lookupMapper;
        assertDataType(lookupMapper.getDataTypeDescription());
    }

    @Override
    public List<List<Values>> batchRetrieve(RedisState redisState, List<TridentTuple> inputs) {
        List<List<Values>> values = new ArrayList<List<Values>>();

        List<String> keys = Lists.newArrayList();
        for (TridentTuple input : inputs) {
            keys.add(lookupMapper.getKeyFromTuple(input));
        }

        Jedis jedis = null;
        try {
            jedis = redisState.getJedis();
            List<String> redisVals = jedis.mget(keys.toArray(new String[keys.size()]));

            for (int i = 0 ; i < redisVals.size() ; i++) {
                values.add(lookupMapper.toTuple(inputs.get(i), redisVals.get(i)));
            }

            return values;
        } finally {
            if (jedis != null) {
                redisState.returnJedis(jedis);
            }
        }
    }

    @Override
    public void execute(TridentTuple tuple, List<Values> values, TridentCollector collector) {
        for (Values value : values) {
            collector.emit(value);
        }
    }

    private void assertDataType(RedisDataTypeDescription lookupMapper) {
        if (lookupMapper.getDataType() != RedisDataTypeDescription.RedisDataType.STRING) {
            throw new IllegalArgumentException("State should be STRING type");
        }
    }
}