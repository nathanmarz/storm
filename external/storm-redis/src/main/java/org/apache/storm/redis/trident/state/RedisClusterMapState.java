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
import com.google.common.collect.Lists;
import org.apache.storm.redis.common.config.JedisClusterConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import redis.clients.jedis.JedisCluster;
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
 * IBackingMap implementation for Redis Cluster environment.
 *
 * @param <T> value's type class
 * @see AbstractRedisMapState
 */
public class RedisClusterMapState<T> extends AbstractRedisMapState<T> {
    /**
     * Provides StateFactory for opaque transactional.
     *
     * @param jedisClusterConfig configuration for JedisCluster
     * @return StateFactory
     */
    public static StateFactory opaque(JedisClusterConfig jedisClusterConfig) {
        return opaque(jedisClusterConfig, new Options());
    }

    /**
     * Provides StateFactory for opaque transactional.
     *
     * @param jedisClusterConfig configuration for JedisCluster
     * @param dataTypeDescription definition of data type
     * @return StateFactory
     */
    public static StateFactory opaque(JedisClusterConfig jedisClusterConfig, RedisDataTypeDescription dataTypeDescription) {
        Options opts = new Options();
        opts.dataTypeDescription = dataTypeDescription;
        return opaque(jedisClusterConfig, opts);
    }

    /**
     * Provides StateFactory for opaque transactional.
     *
     * @param jedisClusterConfig configuration for JedisCluster
     * @param factory key factory
     * @return StateFactory
     */
    public static StateFactory opaque(JedisClusterConfig jedisClusterConfig, KeyFactory factory) {
        Options opts = new Options();
        opts.keyFactory = factory;
        return opaque(jedisClusterConfig, opts);
    }

    /**
     * Provides StateFactory for opaque transactional.
     *
     * @param jedisClusterConfig configuration for JedisCluster
     * @param opts options of State
     * @return StateFactory
     */
    public static StateFactory opaque(JedisClusterConfig jedisClusterConfig, Options<OpaqueValue> opts) {
        return new Factory(jedisClusterConfig, StateType.OPAQUE, opts);
    }

    /**
     * Provides StateFactory for transactional.
     *
     * @param jedisClusterConfig configuration for JedisCluster
     * @return StateFactory
     */
    public static StateFactory transactional(JedisClusterConfig jedisClusterConfig) {
        return transactional(jedisClusterConfig, new Options());
    }

    /**
     * Provides StateFactory for transactional.
     *
     * @param jedisClusterConfig configuration for JedisCluster
     * @param dataTypeDescription definition of data type
     * @return StateFactory
     */
    public static StateFactory transactional(JedisClusterConfig jedisClusterConfig, RedisDataTypeDescription dataTypeDescription) {
        Options opts = new Options();
        opts.dataTypeDescription = dataTypeDescription;
        return transactional(jedisClusterConfig, opts);
    }

    /**
     * Provides StateFactory for transactional.
     *
     * @param jedisClusterConfig configuration for JedisCluster
     * @param factory key factory
     * @return StateFactory
     */
    public static StateFactory transactional(JedisClusterConfig jedisClusterConfig, KeyFactory factory) {
        Options opts = new Options();
        opts.keyFactory = factory;
        return transactional(jedisClusterConfig, opts);
    }

    /**
     * Provides StateFactory for transactional.
     *
     * @param jedisClusterConfig configuration for JedisCluster
     * @param opts options of State
     * @return StateFactory
     */
    public static StateFactory transactional(JedisClusterConfig jedisClusterConfig, Options<TransactionalValue> opts) {
        return new Factory(jedisClusterConfig, StateType.TRANSACTIONAL, opts);
    }

    /**
     * Provides StateFactory for non transactional.
     *
     * @param jedisClusterConfig configuration for JedisCluster
     * @return StateFactory
     */
    public static StateFactory nonTransactional(JedisClusterConfig jedisClusterConfig) {
        return nonTransactional(jedisClusterConfig, new Options());
    }

    /**
     * Provides StateFactory for non transactional.
     *
     * @param jedisClusterConfig configuration for JedisCluster
     * @param dataTypeDescription definition of data type
     * @return StateFactory
     */
    public static StateFactory nonTransactional(JedisClusterConfig jedisClusterConfig, RedisDataTypeDescription dataTypeDescription) {
        Options opts = new Options();
        opts.dataTypeDescription = dataTypeDescription;
        return nonTransactional(jedisClusterConfig, opts);
    }

    /**
     * Provides StateFactory for non transactional.
     *
     * @param jedisClusterConfig configuration for JedisCluster
     * @param factory key factory
     * @return StateFactory
     */
    public static StateFactory nonTransactional(JedisClusterConfig jedisClusterConfig, KeyFactory factory) {
        Options opts = new Options();
        opts.keyFactory = factory;
        return nonTransactional(jedisClusterConfig, opts);
    }

    /**
     * Provides StateFactory for non transactional.
     *
     * @param jedisClusterConfig configuration for JedisCluster
     * @param opts options of State
     * @return StateFactory
     */
    public static StateFactory nonTransactional(JedisClusterConfig jedisClusterConfig, Options<Object> opts) {
        return new Factory(jedisClusterConfig, StateType.NON_TRANSACTIONAL, opts);
    }

    /**
     * RedisClusterMapState.Factory provides Redis Cluster environment version of StateFactory.
     */
    protected static class Factory implements StateFactory {
        public static final redis.clients.jedis.JedisPoolConfig DEFAULT_POOL_CONFIG = new redis.clients.jedis.JedisPoolConfig();

        JedisClusterConfig jedisClusterConfig;

        StateType type;
        Serializer serializer;
        KeyFactory keyFactory;
        Options options;

        /**
         * Constructor
         *
         * @param jedisClusterConfig configuration for JedisCluster
         * @param type StateType
         * @param options options of State
         */
        public Factory(JedisClusterConfig jedisClusterConfig, StateType type, Options options) {
            this.jedisClusterConfig = jedisClusterConfig;
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
            JedisCluster jedisCluster = new JedisCluster(jedisClusterConfig.getNodes(),
                                                            jedisClusterConfig.getTimeout(),
                                                            jedisClusterConfig.getMaxRedirections(),
                                                            DEFAULT_POOL_CONFIG);

            RedisClusterMapState state = new RedisClusterMapState(jedisCluster, options, serializer, keyFactory);
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

    private JedisCluster jedisCluster;
    private Options options;
    private Serializer serializer;
    private KeyFactory keyFactory;

    /**
     * Constructor
     *
     * @param jedisCluster JedisCluster
     * @param options options of State
     * @param serializer Serializer
     * @param keyFactory KeyFactory
     */
    public RedisClusterMapState(JedisCluster jedisCluster, Options options,
                                Serializer<T> serializer, KeyFactory keyFactory) {
        this.jedisCluster = jedisCluster;
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
        RedisDataTypeDescription description = this.options.dataTypeDescription;
        switch (description.getDataType()) {
        case STRING:
            List<String> values = Lists.newArrayList();

            for (String stringKey : keys) {
                String value = jedisCluster.get(stringKey);
                values.add(value);
            }

            return values;

        case HASH:
            return jedisCluster.hmget(description.getAdditionalKey(), stringKeys);

        default:
            throw new IllegalArgumentException("Cannot process such data type: " + description.getDataType());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void updateStatesToRedis(Map<String, String> keyValues) {
        RedisDataTypeDescription description = this.options.dataTypeDescription;
        switch (description.getDataType()) {
        case STRING:
            for (Map.Entry<String, String> kvEntry : keyValues.entrySet()) {
                if(this.options.expireIntervalSec > 0){
                    jedisCluster.setex(kvEntry.getKey(), this.options.expireIntervalSec, kvEntry.getValue());
                } else {
                    jedisCluster.set(kvEntry.getKey(), kvEntry.getValue());
                }
            }
            break;

        case HASH:
            jedisCluster.hmset(description.getAdditionalKey(), keyValues);
            if (this.options.expireIntervalSec > 0) {
                jedisCluster.expire(description.getAdditionalKey(), this.options.expireIntervalSec);
            }
            break;

        default:
            throw new IllegalArgumentException("Cannot process such data type: " + description.getDataType());
        }
    }
}
