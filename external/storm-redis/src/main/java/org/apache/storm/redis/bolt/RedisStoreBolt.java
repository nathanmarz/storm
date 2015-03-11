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
package org.apache.storm.redis.bolt;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import org.apache.storm.redis.common.config.JedisClusterConfig;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import redis.clients.jedis.JedisCommands;

public class RedisStoreBolt extends AbstractRedisBolt {
    private final RedisStoreMapper storeMapper;
    private final RedisDataTypeDescription.RedisDataType dataType;
    private final String additionalKey;

    public RedisStoreBolt(JedisPoolConfig config, RedisStoreMapper storeMapper) {
        super(config);
        this.storeMapper = storeMapper;

        RedisDataTypeDescription dataTypeDescription = storeMapper.getDataTypeDescription();
        this.dataType = dataTypeDescription.getDataType();
        this.additionalKey = dataTypeDescription.getAdditionalKey();
    }

    public RedisStoreBolt(JedisClusterConfig config, RedisStoreMapper storeMapper) {
        super(config);
        this.storeMapper = storeMapper;

        RedisDataTypeDescription dataTypeDescription = storeMapper.getDataTypeDescription();
        this.dataType = dataTypeDescription.getDataType();
        this.additionalKey = dataTypeDescription.getAdditionalKey();
    }

    @Override
    public void execute(Tuple input) {
        String key = storeMapper.getKeyFromTuple(input);
        String value = storeMapper.getValueFromTuple(input);

        JedisCommands jedisCommand = null;
        try {
            jedisCommand = getInstance();

            switch (dataType) {
                case STRING:
                    jedisCommand.set(key, value);
                    break;

                case LIST:
                    jedisCommand.rpush(key, value);
                    break;

                case HASH:
                    jedisCommand.hset(additionalKey, key, value);
                    break;

                case SET:
                    jedisCommand.sadd(key, value);
                    break;

                case SORTED_SET:
                    jedisCommand.zadd(additionalKey, Double.valueOf(value), key);

                case HYPER_LOG_LOG:
                    jedisCommand.pfadd(key, value);
                    break;

                default:
                    throw new IllegalArgumentException("Cannot process such data type: " + dataType);
            }

            collector.ack(input);
        } catch (Exception e) {
            this.collector.reportError(e);
            this.collector.fail(input);
        } finally {
            returnInstance(jedisCommand);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
