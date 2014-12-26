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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;
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
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        public Serializer<T> serializer = null;
        public KeyFactory keyFactory = null;
        public int connectionTimeout = Protocol.DEFAULT_TIMEOUT;
        public int maxRedirections = 10;
        public String password = null;
        public int database = Protocol.DEFAULT_DATABASE;
    }

    public static interface KeyFactory extends Serializable {
        String build(List<Object> key);
    }

    public static StateFactory opaque(Set<HostAndPort> clusterNodes) {
        return opaque(clusterNodes, new Options());
    }

    public static StateFactory opaque(Set<HostAndPort> clusterNodes, Options<OpaqueValue> opts) {
        return opaque(clusterNodes, opts, new DefaultKeyFactory());
    }

    public static StateFactory opaque(Set<HostAndPort> clusterNodes, Options<OpaqueValue> opts, KeyFactory factory) {
        return new Factory(clusterNodes, StateType.OPAQUE, opts, factory);
    }

    public static StateFactory transactional(Set<HostAndPort> clusterNodes) {
        return transactional(clusterNodes, new Options());
    }

    public static StateFactory transactional(Set<HostAndPort> clusterNodes, Options<TransactionalValue> opts) {
        return transactional(clusterNodes, opts, new DefaultKeyFactory());
    }

    public static StateFactory transactional(Set<HostAndPort> clusterNodes, Options<TransactionalValue> opts, KeyFactory factory) {
        return new Factory(clusterNodes, StateType.TRANSACTIONAL, opts, factory);
    }

    public static StateFactory nonTransactional(Set<HostAndPort> clusterNodes) {
        return nonTransactional(clusterNodes, new Options());
    }

    public static StateFactory nonTransactional(Set<HostAndPort> clusterNodes, Options<Object> opts) {
        return nonTransactional(clusterNodes, opts, new DefaultKeyFactory());
    }

    public static StateFactory nonTransactional(Set<HostAndPort> clusterNodes, Options<Object> opts, KeyFactory factory) {
        return new Factory(clusterNodes, StateType.NON_TRANSACTIONAL, opts, factory);
    }

    protected static class Factory implements StateFactory {
        StateType type;
        Set<HostAndPort> clusterNodes;
        Serializer serializer;
        KeyFactory factory;
        Options options;

        public Factory(Set<HostAndPort> clusterNodes, StateType type, Options options, KeyFactory factory) {
            this.type = type;
            this.clusterNodes = clusterNodes;
            this.options = options;
            this.factory = factory;

            if (options.serializer == null) {
                serializer = DEFAULT_SERIALIZERS.get(type);
                if (serializer == null) {
                    throw new RuntimeException("Couldn't find serializer for state type: " + type);
                }
            } else {
                this.serializer = options.serializer;
            }
        }

        public State makeState(@SuppressWarnings("rawtypes") Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            JedisCluster jc = new JedisCluster(clusterNodes, options.connectionTimeout, options.maxRedirections);
            RedisClusterMapState state = new RedisClusterMapState(jc, options, serializer, factory);
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

    private final JedisCluster jedisCluster;
    private Options options;
    private Serializer serializer;
    private KeyFactory keyFactory;

    public RedisClusterMapState(JedisCluster jedisCluster, Options options, Serializer<T> serializer, KeyFactory keyFactory) {
        this.jedisCluster = jedisCluster;
        this.options = options;
        this.serializer = serializer;
        this.keyFactory = keyFactory;
    }

    public List<T> multiGet(List<List<Object>> keys) {
        if (keys.size() == 0) {
            return Collections.emptyList();
        }

        String[] stringKeys = buildKeys(keys);
        List<String> values = mget(stringKeys);
        return deserializeValues(keys, values);
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

    private List<String> mget(String... keys) {
        List<String> rets = new ArrayList<String>();
        for (String key : keys) {
            rets.add(jedisCluster.get(key));
        }
        return rets;
    }

    public void multiPut(List<List<Object>> keys, List<T> vals) {
        if (keys.size() == 0) {
            return;
        }
        if (keys.size() != vals.size()) {
            logger.error("multiPut keys.size[" + keys.size() + "] not equal to vals.size[" + vals.size() + "]");
            return;
        }

        /**
         * Todo: because JedisCluster now not support mset, we use for,set to implement multiPut
         * */
        for (int i = 0; i < keys.size(); i++) {
            String redisKey = keyFactory.build(keys.get(i));
            String val = new String(serializer.serialize(vals.get(i)));
            jedisCluster.set(redisKey, val);
        }

    }
}
