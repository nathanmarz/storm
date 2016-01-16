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

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.redis.common.config.JedisClusterConfig;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import redis.clients.jedis.JedisCommands;

/**
 * Basic bolt for writing to Redis
 * <p/>
 * Various data types are supported: STRING, LIST, HASH, SET, SORTED_SET, HYPER_LOG_LOG
 */
public class RedisStoreBolt extends AbstractRedisBolt {
    private final RedisStoreMapper storeMapper;
    private final RedisDataTypeDescription.RedisDataType dataType;
    private final String additionalKey;

    /**
     * Constructor for single Redis environment (JedisPool)
     * @param config configuration for initializing JedisPool
     * @param storeMapper mapper containing which datatype, storing value's key that Bolt uses
     */
    public RedisStoreBolt(JedisPoolConfig config, RedisStoreMapper storeMapper) {
        super(config);
        this.storeMapper = storeMapper;

        RedisDataTypeDescription dataTypeDescription = storeMapper.getDataTypeDescription();
        this.dataType = dataTypeDescription.getDataType();
        this.additionalKey = dataTypeDescription.getAdditionalKey();
    }

    /**
     * Constructor for Redis Cluster environment (JedisCluster)
     * @param config configuration for initializing JedisCluster
     * @param storeMapper mapper containing which datatype, storing value's key that Bolt uses
     */
    public RedisStoreBolt(JedisClusterConfig config, RedisStoreMapper storeMapper) {
        super(config);
        this.storeMapper = storeMapper;

        RedisDataTypeDescription dataTypeDescription = storeMapper.getDataTypeDescription();
        this.dataType = dataTypeDescription.getDataType();
        this.additionalKey = dataTypeDescription.getAdditionalKey();
    }

    /**
     * {@inheritDoc}
     */
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
                    break;

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

    /**
     * {@inheritDoc}
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
