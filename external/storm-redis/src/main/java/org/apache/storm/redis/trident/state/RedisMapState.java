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

import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Values;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import storm.trident.state.OpaqueValue;
import storm.trident.state.Serializer;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.TransactionalValue;
import storm.trident.state.map.CachedMap;
import storm.trident.state.map.MapState;
import storm.trident.state.map.NonTransactionalMap;
import storm.trident.state.map.OpaqueMap;
import storm.trident.state.map.SnapshottableMap;
import storm.trident.state.map.TransactionalMap;

import java.util.List;
import java.util.Map;

public class RedisMapState<T> extends AbstractRedisMapState<T> {
    /**
     * OpaqueTransactional for redis.
     * */
    public static StateFactory opaque(JedisPoolConfig jedisPoolConfig) {
        return opaque(jedisPoolConfig, new Options());
    }

    public static StateFactory opaque(JedisPoolConfig jedisPoolConfig, RedisDataTypeDescription dataTypeDescription) {
        Options opts = new Options();
        opts.dataTypeDescription = dataTypeDescription;
        return opaque(jedisPoolConfig, opts);
    }

    public static StateFactory opaque(JedisPoolConfig jedisPoolConfig,  KeyFactory factory) {
        Options opts = new Options();
        opts.keyFactory = factory;
        return opaque(jedisPoolConfig, opts);
    }

    public static StateFactory opaque(JedisPoolConfig jedisPoolConfig, Options<OpaqueValue> opts) {
        return new Factory(jedisPoolConfig, StateType.OPAQUE, opts);
    }

    /**
     * Transactional for redis.
     * */
    public static StateFactory transactional(JedisPoolConfig jedisPoolConfig) {
        return transactional(jedisPoolConfig, new Options());
    }

    public static StateFactory transactional(JedisPoolConfig jedisPoolConfig, RedisDataTypeDescription dataTypeDescription) {
        Options opts = new Options();
        opts.dataTypeDescription = dataTypeDescription;
        return transactional(jedisPoolConfig, opts);
    }

    public static StateFactory transactional(JedisPoolConfig jedisPoolConfig, KeyFactory factory) {
        Options opts = new Options();
        opts.keyFactory = factory;
        return transactional(jedisPoolConfig, opts);
    }

    public static StateFactory transactional(JedisPoolConfig jedisPoolConfig, Options<TransactionalValue> opts) {
        return new Factory(jedisPoolConfig, StateType.TRANSACTIONAL, opts);
    }

    /**
     * NonTransactional for redis.
     * */
    public static StateFactory nonTransactional(JedisPoolConfig jedisPoolConfig) {
        return nonTransactional(jedisPoolConfig, new Options());
    }

    public static StateFactory nonTransactional(JedisPoolConfig jedisPoolConfig, RedisDataTypeDescription dataTypeDescription) {
        Options opts = new Options();
        opts.dataTypeDescription = dataTypeDescription;
        return nonTransactional(jedisPoolConfig, opts);
    }

    public static StateFactory nonTransactional(JedisPoolConfig jedisPoolConfig, KeyFactory factory) {
        Options opts = new Options();
        opts.keyFactory = factory;
        return nonTransactional(jedisPoolConfig, opts);
    }

    public static StateFactory nonTransactional(JedisPoolConfig jedisPoolConfig, Options<Object> opts) {
        return new Factory(jedisPoolConfig, StateType.NON_TRANSACTIONAL, opts);
    }

    protected static class Factory implements StateFactory {
        public static final redis.clients.jedis.JedisPoolConfig DEFAULT_POOL_CONFIG = new redis.clients.jedis.JedisPoolConfig();

        JedisPoolConfig jedisPoolConfig;

        StateType type;
        Serializer serializer;
        KeyFactory keyFactory;
        Options options;

        public Factory(JedisPoolConfig jedisPoolConfig, StateType type, Options options) {
            this.jedisPoolConfig = jedisPoolConfig;
            this.type = type;
            this.options = options;

            this.keyFactory = options.keyFactory;
            if (this.keyFactory == null) {
                this.keyFactory = new KeyFactory.DefaultKeyFactory();
            }
            this.serializer = options.serializer;
            if (this.serializer == null) {
                this.serializer = DEFAULT_SERIALIZERS.get(type);
                if (this.serializer == null) {
                    throw new RuntimeException("Couldn't find serializer for state type: " + type);
                }
            }
        }

        public State makeState(@SuppressWarnings("rawtypes") Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            JedisPool jedisPool = new JedisPool(DEFAULT_POOL_CONFIG,
                                                    jedisPoolConfig.getHost(),
                                                    jedisPoolConfig.getPort(),
                                                    jedisPoolConfig.getTimeout(),
                                                    jedisPoolConfig.getPassword(),
                                                    jedisPoolConfig.getDatabase());
            RedisMapState state = new RedisMapState(jedisPool, options, serializer, keyFactory);
            CachedMap c = new CachedMap(state, options.localCacheSize);

            MapState ms;
            if (type == StateType.NON_TRANSACTIONAL) {
                ms = NonTransactionalMap.build(c);

            } else if (type == StateType.OPAQUE) {
                ms = OpaqueMap.build(c);

            } else if (type == StateType.TRANSACTIONAL) {
                ms = TransactionalMap.build(c);

            } else {
                throw new RuntimeException("Unknown state type: " + type);
            }

            return new SnapshottableMap(ms, new Values(options.globalKey));
        }
    }

    private JedisPool jedisPool;
    private Options options;
    private Serializer serializer;
    private KeyFactory keyFactory;

    public RedisMapState(JedisPool jedisPool, Options options,
                                            Serializer<T> serializer, KeyFactory keyFactory) {
        this.jedisPool = jedisPool;
        this.options = options;
        this.serializer = serializer;
        this.keyFactory = keyFactory;
    }

    @Override
    protected Serializer getSerializer() {
        return serializer;
    }

    @Override
    protected KeyFactory getKeyFactory() {
        return keyFactory;
    }

    @Override
    protected List<String> retrieveValuesFromRedis(List<String> keys) {
        String[] stringKeys = keys.toArray(new String[keys.size()]);

        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();

            RedisDataTypeDescription description = this.options.dataTypeDescription;
            switch (description.getDataType()) {
            case STRING:
                return jedis.mget(stringKeys);

            case HASH:
                return jedis.hmget(description.getAdditionalKey(), stringKeys);

            default:
                throw new IllegalArgumentException("Cannot process such data type: " + description.getDataType());
            }

        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    protected void updateStatesToRedis(Map<String, String> keyValues) {
        Jedis jedis = null;

        try {
            jedis = jedisPool.getResource();

            RedisDataTypeDescription description = this.options.dataTypeDescription;
            switch (description.getDataType()) {
            case STRING:
                String[] keyValue = buildKeyValuesList(keyValues);
                jedis.mset(keyValue);
                if(this.options.expireIntervalSec > 0){
                    Pipeline pipe = jedis.pipelined();
                    for(int i = 0; i < keyValue.length; i += 2){
                        pipe.expire(keyValue[i], this.options.expireIntervalSec);
                    }
                    pipe.sync();
                }
                break;

            case HASH:
                jedis.hmset(description.getAdditionalKey(), keyValues);
                if (this.options.expireIntervalSec > 0) {
                    jedis.expire(description.getAdditionalKey(), this.options.expireIntervalSec);
                }
                break;

            default:
                throw new IllegalArgumentException("Cannot process such data type: " + description.getDataType());
            }

        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    private String[] buildKeyValuesList(Map<String, String> keyValues) {
        String[] keyValueLists = new String[keyValues.size() * 2];

        int idx = 0;
        for (Map.Entry<String, String> kvEntry : keyValues.entrySet()) {
            keyValueLists[idx++] = kvEntry.getKey();
            keyValueLists[idx++] = kvEntry.getValue();
        }

        return keyValueLists;
    }
}
