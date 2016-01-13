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

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.tuple.Values;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import org.apache.storm.trident.state.OpaqueValue;
import org.apache.storm.trident.state.Serializer;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.StateType;
import org.apache.storm.trident.state.TransactionalValue;
import org.apache.storm.trident.state.map.CachedMap;
import org.apache.storm.trident.state.map.MapState;
import org.apache.storm.trident.state.map.NonTransactionalMap;
import org.apache.storm.trident.state.map.OpaqueMap;
import org.apache.storm.trident.state.map.SnapshottableMap;
import org.apache.storm.trident.state.map.TransactionalMap;

import java.util.List;
import java.util.Map;

/**
 * IBackingMap implementation for single Redis environment.
 *
 * @param <T> value's type class
 * @see AbstractRedisMapState
 */
public class RedisMapState<T> extends AbstractRedisMapState<T> {
    /**
     * Provides StateFactory for opaque transactional.
     *
     * @param jedisPoolConfig configuration for JedisPool
     * @return StateFactory
     */
    public static StateFactory opaque(JedisPoolConfig jedisPoolConfig) {
        return opaque(jedisPoolConfig, new Options());
    }

    /**
     * Provides StateFactory for opaque transactional.
     *
     * @param jedisPoolConfig configuration for JedisPool
     * @param dataTypeDescription definition of data type
     * @return StateFactory
     */
    public static StateFactory opaque(JedisPoolConfig jedisPoolConfig, RedisDataTypeDescription dataTypeDescription) {
        Options opts = new Options();
        opts.dataTypeDescription = dataTypeDescription;
        return opaque(jedisPoolConfig, opts);
    }

    /**
     * Provides StateFactory for opaque transactional.
     *
     * @param jedisPoolConfig configuration for JedisPool
     * @param factory key factory
     * @return StateFactory
     */
    public static StateFactory opaque(JedisPoolConfig jedisPoolConfig, KeyFactory factory) {
        Options opts = new Options();
        opts.keyFactory = factory;
        return opaque(jedisPoolConfig, opts);
    }

    /**
     * Provides StateFactory for opaque transactional.
     *
     * @param jedisPoolConfig configuration for JedisPool
     * @param opts options of State
     * @return StateFactory
     */
    public static StateFactory opaque(JedisPoolConfig jedisPoolConfig, Options<OpaqueValue> opts) {
        return new Factory(jedisPoolConfig, StateType.OPAQUE, opts);
    }

    /**
     * Provides StateFactory for transactional.
     *
     * @param jedisPoolConfig configuration for JedisPool
     * @return StateFactory
     */
    public static StateFactory transactional(JedisPoolConfig jedisPoolConfig) {
        return transactional(jedisPoolConfig, new Options());
    }

    /**
     * Provides StateFactory for transactional.
     *
     * @param jedisPoolConfig configuration for JedisPool
     * @param dataTypeDescription definition of data type
     * @return StateFactory
     */
    public static StateFactory transactional(JedisPoolConfig jedisPoolConfig, RedisDataTypeDescription dataTypeDescription) {
        Options opts = new Options();
        opts.dataTypeDescription = dataTypeDescription;
        return transactional(jedisPoolConfig, opts);
    }

    /**
     * Provides StateFactory for transactional.
     *
     * @param jedisPoolConfig configuration for JedisPool
     * @param factory key factory
     * @return StateFactory
     */
    public static StateFactory transactional(JedisPoolConfig jedisPoolConfig, KeyFactory factory) {
        Options opts = new Options();
        opts.keyFactory = factory;
        return transactional(jedisPoolConfig, opts);
    }

    /**
     * Provides StateFactory for transactional.
     *
     * @param jedisPoolConfig configuration for JedisPool
     * @param opts options of State
     * @return StateFactory
     */
    public static StateFactory transactional(JedisPoolConfig jedisPoolConfig, Options<TransactionalValue> opts) {
        return new Factory(jedisPoolConfig, StateType.TRANSACTIONAL, opts);
    }

    /**
     * Provides StateFactory for non transactional.
     *
     * @param jedisPoolConfig configuration for JedisPool
     * @return StateFactory
     */
    public static StateFactory nonTransactional(JedisPoolConfig jedisPoolConfig) {
        return nonTransactional(jedisPoolConfig, new Options());
    }

    /**
     * Provides StateFactory for non transactional.
     *
     * @param jedisPoolConfig configuration for JedisPool
     * @param dataTypeDescription definition of data type
     * @return StateFactory
     */
    public static StateFactory nonTransactional(JedisPoolConfig jedisPoolConfig, RedisDataTypeDescription dataTypeDescription) {
        Options opts = new Options();
        opts.dataTypeDescription = dataTypeDescription;
        return nonTransactional(jedisPoolConfig, opts);
    }

    /**
     * Provides StateFactory for non transactional.
     *
     * @param jedisPoolConfig configuration for JedisPool
     * @param factory key factory
     * @return StateFactory
     */
    public static StateFactory nonTransactional(JedisPoolConfig jedisPoolConfig, KeyFactory factory) {
        Options opts = new Options();
        opts.keyFactory = factory;
        return nonTransactional(jedisPoolConfig, opts);
    }

    /**
     * Provides StateFactory for non transactional.
     *
     * @param jedisPoolConfig configuration for JedisPool
     * @param opts options of State
     * @return StateFactory
     */
    public static StateFactory nonTransactional(JedisPoolConfig jedisPoolConfig, Options<Object> opts) {
        return new Factory(jedisPoolConfig, StateType.NON_TRANSACTIONAL, opts);
    }

    /**
     * RedisMapState.Factory provides single Redis environment version of StateFactory.
     */
    protected static class Factory implements StateFactory {
        public static final redis.clients.jedis.JedisPoolConfig DEFAULT_POOL_CONFIG = new redis.clients.jedis.JedisPoolConfig();

        JedisPoolConfig jedisPoolConfig;

        StateType type;
        Serializer serializer;
        KeyFactory keyFactory;
        Options options;

        /**
         * Constructor
         *
         * @param jedisPoolConfig configuration for JedisPool
         * @param type StateType
         * @param options options of State
         */
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

        /**
         * {@inheritDoc}
         */
        @Override
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

    /**
     * Constructor
     *
     * @param jedisPool JedisPool
     * @param options options of State
     * @param serializer Serializer
     * @param keyFactory KeyFactory
     */
    public RedisMapState(JedisPool jedisPool, Options options,
                                            Serializer<T> serializer, KeyFactory keyFactory) {
        this.jedisPool = jedisPool;
        this.options = options;
        this.serializer = serializer;
        this.keyFactory = keyFactory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Serializer getSerializer() {
        return serializer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected KeyFactory getKeyFactory() {
        return keyFactory;
    }

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
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
