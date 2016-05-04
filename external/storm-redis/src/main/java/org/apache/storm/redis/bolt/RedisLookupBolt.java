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
import org.apache.storm.tuple.Values;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisLookupMapper;
import org.apache.storm.redis.common.config.JedisClusterConfig;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import redis.clients.jedis.JedisCommands;

import java.util.List;

/**
 * Basic bolt for querying from Redis and emits response as tuple.
 * <p/>
 * Various data types are supported: STRING, LIST, HASH, SET, SORTED_SET, HYPER_LOG_LOG
 */
public class RedisLookupBolt extends AbstractRedisBolt {
    private final RedisLookupMapper lookupMapper;
    private final RedisDataTypeDescription.RedisDataType dataType;
    private final String additionalKey;

    /**
     * Constructor for single Redis environment (JedisPool)
     * @param config configuration for initializing JedisPool
     * @param lookupMapper mapper containing which datatype, query key, output key that Bolt uses
     */
    public RedisLookupBolt(JedisPoolConfig config, RedisLookupMapper lookupMapper) {
        super(config);

        this.lookupMapper = lookupMapper;

        RedisDataTypeDescription dataTypeDescription = lookupMapper.getDataTypeDescription();
        this.dataType = dataTypeDescription.getDataType();
        this.additionalKey = dataTypeDescription.getAdditionalKey();
    }

    /**
     * Constructor for Redis Cluster environment (JedisCluster)
     * @param config configuration for initializing JedisCluster
     * @param lookupMapper mapper containing which datatype, query key, output key that Bolt uses
     */
    public RedisLookupBolt(JedisClusterConfig config, RedisLookupMapper lookupMapper) {
        super(config);

        this.lookupMapper = lookupMapper;

        RedisDataTypeDescription dataTypeDescription = lookupMapper.getDataTypeDescription();
        this.dataType = dataTypeDescription.getDataType();
        this.additionalKey = dataTypeDescription.getAdditionalKey();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(Tuple input) {
        String key = lookupMapper.getKeyFromTuple(input);
        Object lookupValue;

        JedisCommands jedisCommand = null;
        try {
            jedisCommand = getInstance();

            switch (dataType) {
                case STRING:
                    lookupValue = jedisCommand.get(key);
                    break;

                case LIST:
                    lookupValue = jedisCommand.lpop(key);
                    break;

                case HASH:
                    lookupValue = jedisCommand.hget(additionalKey, key);
                    break;

                case SET:
                    lookupValue = jedisCommand.scard(key);
                    break;

                case SORTED_SET:
                    lookupValue = jedisCommand.zscore(additionalKey, key);
                    break;

                case HYPER_LOG_LOG:
                    lookupValue = jedisCommand.pfcount(key);
                    break;

                default:
                    throw new IllegalArgumentException("Cannot process such data type: " + dataType);
            }

            List<Values> values = lookupMapper.toTuple(input, lookupValue);
            for (Values value : values) {
                collector.emit(input, value);
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
        lookupMapper.declareOutputFields(declarer);
    }
}
