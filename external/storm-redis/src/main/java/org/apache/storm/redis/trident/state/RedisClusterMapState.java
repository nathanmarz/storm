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
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.storm.redis.common.config.JedisClusterConfig;
import redis.clients.jedis.JedisCluster;
import storm.trident.state.*;
import storm.trident.state.map.*;

import java.util.List;
import java.util.Map;

public class RedisClusterMapState<T> extends AbstractRedisMapState<T> {
    /**
     * OpaqueTransactional for redis-cluster.
     * */
    public static StateFactory opaque(JedisClusterConfig jedisClusterConfig) {
        return opaque(jedisClusterConfig, new Options());
    }

    public static StateFactory opaque(JedisClusterConfig jedisClusterConfig, String hkey) {
        Options opts = new Options();
        opts.hkey = hkey;
        return opaque(jedisClusterConfig, opts);
    }

    public static StateFactory opaque(JedisClusterConfig jedisClusterConfig, KeyFactory factory) {
        Options opts = new Options();
        opts.keyFactory = factory;
        return opaque(jedisClusterConfig, opts);
    }

    public static StateFactory opaque(JedisClusterConfig jedisClusterConfig, Options<OpaqueValue> opts) {
        return new Factory(jedisClusterConfig, StateType.OPAQUE, opts);
    }

    /**
     * Transactional for redis-cluster.
     * */
    public static StateFactory transactional(JedisClusterConfig jedisClusterConfig) {
        return transactional(jedisClusterConfig, new Options());
    }

    public static StateFactory transactional(JedisClusterConfig jedisClusterConfig, String hkey) {
        Options opts = new Options();
        opts.hkey = hkey;
        return transactional(jedisClusterConfig, opts);
    }

    public static StateFactory transactional(JedisClusterConfig jedisClusterConfig, KeyFactory factory) {
        Options opts = new Options();
        opts.keyFactory = factory;
        return transactional(jedisClusterConfig, opts);
    }

    public static StateFactory transactional(JedisClusterConfig jedisClusterConfig, Options<TransactionalValue> opts) {
        return new Factory(jedisClusterConfig, StateType.TRANSACTIONAL, opts);
    }

    /**
     * NonTransactional for redis-cluster.
     * */
    public static StateFactory nonTransactional(JedisClusterConfig jedisClusterConfig) {
        return nonTransactional(jedisClusterConfig, new Options());
    }

    public static StateFactory nonTransactional(JedisClusterConfig jedisClusterConfig, String hkey) {
        Options opts = new Options();
        opts.hkey = hkey;
        return nonTransactional(jedisClusterConfig, opts);
    }

    public static StateFactory nonTransactional(JedisClusterConfig jedisClusterConfig, KeyFactory factory) {
        Options opts = new Options();
        opts.keyFactory = factory;
        return nonTransactional(jedisClusterConfig, opts);
    }

    public static StateFactory nonTransactional(JedisClusterConfig jedisClusterConfig, Options<Object> opts) {
        return new Factory(jedisClusterConfig, StateType.NON_TRANSACTIONAL, opts);
    }

    protected static class Factory implements StateFactory {
        public static final redis.clients.jedis.JedisPoolConfig DEFAULT_POOL_CONFIG = new redis.clients.jedis.JedisPoolConfig();

        JedisClusterConfig jedisClusterConfig;

        StateType type;
        Serializer serializer;
        KeyFactory keyFactory;
        Options options;

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

    public RedisClusterMapState(JedisCluster jedisCluster, Options options,
                                Serializer<T> serializer, KeyFactory keyFactory) {
        this.jedisCluster = jedisCluster;
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
        if (Strings.isNullOrEmpty(this.options.hkey)) {
            List<String> values = Lists.newArrayList();

            for (String stringKey : keys) {
                String value = jedisCluster.get(stringKey);
                values.add(value);
            }

            return values;
        } else {
            return jedisCluster.hmget(this.options.hkey, stringKeys);
        }
    }

    @Override
    protected void updateStatesToRedis(Map<String, String> keyValues) {
        if (Strings.isNullOrEmpty(this.options.hkey)) {
            for (Map.Entry<String, String> kvEntry : keyValues.entrySet()) {
                jedisCluster.set(kvEntry.getKey(), kvEntry.getValue());
            }
        } else {
            jedisCluster.hmset(this.options.hkey, keyValues);
        }
    }
}
