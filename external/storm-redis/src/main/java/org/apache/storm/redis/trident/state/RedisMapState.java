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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
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
      public Serializer<T> serializer = null;
      public KeyFactory keyFactory = null;
      public int connectionTimeout = Protocol.DEFAULT_TIMEOUT;
      public String password = null;
      public int database = Protocol.DEFAULT_DATABASE;
      public String hkey = null;
   }

   public static interface KeyFactory extends Serializable {
      String build(List<Object> key);
   }

   public static StateFactory opaque(InetSocketAddress server) {
      return opaque(server, new Options());
   }

   public static StateFactory opaque(InetSocketAddress server, String hkey) {
      Options opts = new Options();
      opts.hkey = hkey;
      return opaque(server, opts);
   }

   public static StateFactory opaque(InetSocketAddress server, Options<OpaqueValue> opts) {
      return opaque(server, opts, new DefaultKeyFactory());
   }

   public static StateFactory opaque(InetSocketAddress server, Options<OpaqueValue> opts, KeyFactory factory) {
      return new Factory(server, StateType.OPAQUE, opts, factory);
   }

   public static StateFactory transactional(InetSocketAddress server) {
      return transactional(server, new Options());
   }

   public static StateFactory transactional(InetSocketAddress server, String hkey) {
      Options opts = new Options();
      opts.hkey = hkey;
      return transactional(server, opts);
   }

   public static StateFactory transactional(InetSocketAddress server, Options<TransactionalValue> opts) {
      return transactional(server, opts, new DefaultKeyFactory());
   }

   public static StateFactory transactional(InetSocketAddress server, Options<TransactionalValue> opts, KeyFactory factory) {
      return new Factory(server, StateType.TRANSACTIONAL, opts, factory);
   }

   public static StateFactory nonTransactional(InetSocketAddress server) {
      return nonTransactional(server, new Options());
   }

   public static StateFactory nonTransactional(InetSocketAddress server, String hkey) {
      Options opts = new Options();
      opts.hkey = hkey;
      return nonTransactional(server, opts);
   }

   public static StateFactory nonTransactional(InetSocketAddress server, Options<Object> opts) {
      return nonTransactional(server, opts, new DefaultKeyFactory());
   }

   public static StateFactory nonTransactional(InetSocketAddress server, Options<Object> opts, KeyFactory factory) {
      return new Factory(server, StateType.NON_TRANSACTIONAL, opts, factory);
   }

   protected static class Factory implements StateFactory {
      StateType type;
      InetSocketAddress server;
      Serializer serializer;
      KeyFactory factory;
      Options options;

      public Factory(InetSocketAddress server, StateType type, Options options, KeyFactory factory) {
         this.type = type;
         this.server = server;
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
         JedisPool pool = new JedisPool(new JedisPoolConfig(),
                    server.getHostName(), server.getPort(),
                    options.connectionTimeout,
                    options.password,
                    options.database);
         RedisMapState state = new RedisMapState(pool, options, serializer, factory);
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

   private final JedisPool pool;
   private Options options;
   private Serializer serializer;
   private KeyFactory keyFactory;

   public RedisMapState(JedisPool pool, Options options, Serializer<T> serializer, KeyFactory keyFactory) {
      this.pool = pool;
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
         List<String> values = mget(stringKeys);
         return deserializeValues(keys, values);
      } else {
         Map<byte[], byte[]> keyValue = hgetAll(this.options.hkey.getBytes());
         List<String> values = buildValuesFromMap(keys, keyValue);
         return deserializeValues(keys, values);
      }
   }

   private List<String> buildValuesFromMap(List<List<Object>> keys, Map<byte[], byte[]> keyValue) {
      List<String> values = new ArrayList<String>(keys.size());
      for (List<Object> key : keys) {
         String strKey = keyFactory.build(key);
         byte[] bytes = keyValue.get(strKey.getBytes());
         values.add(new String(bytes));
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

   private List<String> mget(String... keys) {
      Jedis jedis = pool.getResource();
      try {
         return jedis.mget(keys);
      } finally {
         pool.returnResource(jedis);
      }
   }

   private Map<byte[], byte[]> hgetAll(byte[] bytes) {
      Jedis jedis = pool.getResource();
      try {
         return jedis.hgetAll(bytes);
      } finally {
         pool.returnResource(jedis);
      }
   }

   public void multiPut(List<List<Object>> keys, List<T> vals) {
      if (keys.size() == 0) {
         return;
      }

      if (Strings.isNullOrEmpty(this.options.hkey)) {
         String[] keyValues = buildKeyValuesList(keys, vals);
         mset(keyValues);
      } else {
         Jedis jedis = pool.getResource();
         try {
            Pipeline pl = jedis.pipelined();
            pl.multi();

            for (int i = 0; i < keys.size(); i++) {
               String val = new String(serializer.serialize(vals.get(i)));
               pl.hset(this.options.hkey,
                     keyFactory.build(keys.get(i)),
                     val);
            }

            pl.exec();
            pl.sync();
         } finally {
            pool.returnResource(jedis);
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

   private void mset(String... keyValues) {
      Jedis jedis = pool.getResource();
      try {
         jedis.mset(keyValues);
      } finally {
         pool.returnResource(jedis);
      }
   }
}
