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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import storm.trident.state.JSONNonTransactionalSerializer;
import storm.trident.state.JSONOpaqueSerializer;
import storm.trident.state.JSONTransactionalSerializer;
import storm.trident.state.OpaqueValue;
import storm.trident.state.Serializer;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.TransactionalValue;
import storm.trident.state.map.CachedMap;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.MapState;
import storm.trident.state.map.NonTransactionalMap;
import storm.trident.state.map.OpaqueMap;
import storm.trident.state.map.SnapshottableMap;
import storm.trident.state.map.TransactionalMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisMapState<T> implements IBackingMap<T> {
    private static final Logger logger = LoggerFactory.getLogger(RedisMapState.class);

    private static final EnumMap<StateType, Serializer> DEFAULT_SERIALIZERS = Maps.newEnumMap(ImmutableMap.of(
            StateType.NON_TRANSACTIONAL, new JSONNonTransactionalSerializer(),
            StateType.TRANSACTIONAL, new JSONTransactionalSerializer(),
            StateType.OPAQUE, new JSONOpaqueSerializer()
    ));

    public static class DefaultKeyFactory implements KeyFactory {
        public String build(List<Object> key) {
            if (key.size() != 1)
                throw new RuntimeException("Default KeyFactory does not support compound keys");
            return (String) key.get(0);
        }
    };

    public static class Options<T> implements Serializable {
        public int localCacheSize = 1000;
        public String globalKey = "$REDIS-MAP-STATE-GLOBAL";
        KeyFactory keyFactory = null;
        public Serializer<T> serializer = null;
        public String hkey = null;
    }

    public static interface KeyFactory extends Serializable {
        String build(List<Object> key);
    }

    /**
     * OpaqueTransactional for redis.
     * */
    public static StateFactory opaque(JedisPoolConfig jedisPoolConfig) {
        return opaque(jedisPoolConfig, new Options());
    }

    public static StateFactory opaque(JedisPoolConfig jedisPoolConfig, String hkey) {
        Options opts = new Options();
        opts.hkey = hkey;
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

    public static StateFactory transactional(JedisPoolConfig jedisPoolConfig, String hkey) {
        Options opts = new Options();
        opts.hkey = hkey;
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

    public static StateFactory nonTransactional(JedisPoolConfig jedisPoolConfig, String hkey) {
        Options opts = new Options();
        opts.hkey = hkey;
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
                this.keyFactory = new DefaultKeyFactory();
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

    public List<T> multiGet(List<List<Object>> keys) {
        if (keys.size() == 0) {
            return Collections.emptyList();
        }

        String[] stringKeys = buildKeys(keys);

        if (Strings.isNullOrEmpty(this.options.hkey)) {
            Jedis jedis = null;
            try {
                jedis = jedisPool.getResource();
                List<String> values = jedis.mget(stringKeys);
                return deserializeValues(keys, values);
            } finally {
                if (jedis != null) {
                    jedisPool.returnResource(jedis);
                }
            }
        } else {
            Jedis jedis = null;
            try {
                jedis = jedisPool.getResource();
                List<String> values = jedis.hmget(this.options.hkey, stringKeys);
                return deserializeValues(keys, values);
            } finally {
                if (jedis != null) {
                    jedisPool.returnResource(jedis);
                }
            }
        }
    }

    private List<T> deserializeValues(List<List<Object>> keys, List<String> values) {
        List<T> result = new ArrayList<T>(keys.size());
        for (String value : values) {
            if (value != null) {
                result.add((T) serializer.deserialize(value.getBytes()));
            } else {
                result.add(null);
            }
        }
        return result;
    }

    private String[] buildKeys(List<List<Object>> keys) {
        String[] stringKeys = new String[keys.size()];
        int index = 0;
        for (List<Object> key : keys)
            stringKeys[index++] = keyFactory.build(key);
        return stringKeys;
    }

    public void multiPut(List<List<Object>> keys, List<T> vals) {
        if (keys.size() == 0) {
            return;
        }

        if (Strings.isNullOrEmpty(this.options.hkey)) {
            Jedis jedis = null;
            try {
                jedis = jedisPool.getResource();
                String[] keyValue = buildKeyValuesList(keys, vals);
                jedis.mset(keyValue);
            } finally {
                if (jedis != null) {
                    jedisPool.returnResource(jedis);
                }
            }
        } else {
            Jedis jedis = jedisPool.getResource();
            try {
                Map<String, String> keyValues = new HashMap<String, String>();
                for (int i = 0; i < keys.size(); i++) {
                    String val = new String(serializer.serialize(vals.get(i)));
                    keyValues.put(keyFactory.build(keys.get(i)), val);
                }
                jedis.hmset(this.options.hkey, keyValues);

            } finally {
                jedisPool.returnResource(jedis);
            }
        }
    }

    private String[] buildKeyValuesList(List<List<Object>> keys, List<T> vals) {
        String[] keyValues = new String[keys.size() * 2];
        for (int i = 0; i < keys.size(); i++) {
            keyValues[i * 2] = keyFactory.build(keys.get(i));
            keyValues[i * 2 + 1] = new String(serializer.serialize(vals.get(i)));
        }
        return keyValues;
    }
}
