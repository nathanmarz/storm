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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.storm.redis.util.config.JedisClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;
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

public class RedisClusterMapState<T> implements IBackingMap<T> {
    private static final Logger logger = LoggerFactory.getLogger(RedisClusterMapState.class);

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
        // TODO : serialize redis.clients.jedis.JedisPoolConfig
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

    public List<T> multiGet(List<List<Object>> keys) {
        if (keys.size() == 0) {
            return Collections.emptyList();
        }
        if (Strings.isNullOrEmpty(this.options.hkey)) {
            String[] stringKeys = buildKeys(keys);
            List<String> values = Lists.newArrayList();

            for (String stringKey : stringKeys) {
                String value = jedisCluster.get(stringKey);
                values.add(value);
            }

            return deserializeValues(keys, values);
        } else {
            Map<String, String> keyValue = jedisCluster.hgetAll(this.options.hkey);
            List<String> values = buildValuesFromMap(keys, keyValue);
            return deserializeValues(keys, values);
        }
    }

    private List<String> buildValuesFromMap(List<List<Object>> keys, Map<String, String> keyValue) {
        List<String> values = new ArrayList<String>(keys.size());
        for (List<Object> key : keys) {
            String strKey = keyFactory.build(key);
            String value = keyValue.get(strKey);
            values.add(value);
        }
        return values;
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
            for (int i = 0; i < keys.size(); i++) {
                String val = new String(serializer.serialize(vals.get(i)));
                String redisKey = keyFactory.build(keys.get(i));
                jedisCluster.set(redisKey, val);
            }
        } else {
            Map<String, String> keyValues = new HashMap<String, String>();
            for (int i = 0; i < keys.size(); i++) {
                String val = new String(serializer.serialize(vals.get(i)));
                keyValues.put(keyFactory.build(keys.get(i)), val);
            }
            jedisCluster.hmset(this.options.hkey, keyValues);
        }
    }
}
