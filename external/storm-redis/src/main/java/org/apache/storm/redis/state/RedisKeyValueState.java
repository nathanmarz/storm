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
package org.apache.storm.redis.state;

import org.apache.storm.state.DefaultStateSerializer;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.state.Serializer;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.container.JedisCommandsContainerBuilder;
import org.apache.storm.redis.common.container.JedisCommandsInstanceContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCommands;
import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A redis based implementation that persists the state in Redis.
 */
public class RedisKeyValueState<K, V> implements KeyValueState<K, V> {
    private static final Logger LOG = LoggerFactory.getLogger(RedisKeyValueState.class);
    private static final String COMMIT_TXID_KEY = "commit";
    private static final String PREPARE_TXID_KEY = "prepare";

    private final BASE64Encoder base64Encoder;
    private final BASE64Decoder base64Decoder;
    private final String namespace;
    private final String prepareNamespace;
    private final String txidNamespace;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final JedisCommandsInstanceContainer jedisContainer;
    private Map<String, String> pendingPrepare;
    private Map<String, String> pendingCommit;
    private Map<String, String> txIds;

    public RedisKeyValueState(String namespace) {
        this(namespace, new JedisPoolConfig.Builder().build());
    }

    public RedisKeyValueState(String namespace, JedisPoolConfig poolConfig) {
        this(namespace, poolConfig, new DefaultStateSerializer<K>(), new DefaultStateSerializer<V>());
    }

    public RedisKeyValueState(String namespace, JedisPoolConfig poolConfig, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(namespace, JedisCommandsContainerBuilder.build(poolConfig), keySerializer, valueSerializer);
    }

    public RedisKeyValueState(String namespace, JedisCommandsInstanceContainer jedisContainer,
                              Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        base64Encoder = new BASE64Encoder();
        base64Decoder = new BASE64Decoder();
        this.namespace = namespace;
        this.prepareNamespace = namespace + "$prepare";
        this.txidNamespace = namespace + "$txid";
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.jedisContainer = jedisContainer;
        this.pendingPrepare = new ConcurrentHashMap<>();
        initTxids();
        initPendingCommit();
    }

    private void initTxids() {
        JedisCommands commands = null;
        try {
            commands = jedisContainer.getInstance();
            if (commands.exists(txidNamespace)) {
                txIds = commands.hgetAll(txidNamespace);
            } else {
                txIds = new HashMap<>();
            }
            LOG.debug("initTxids, txIds {}", txIds);
        } finally {
            jedisContainer.returnInstance(commands);
        }
    }

    private void initPendingCommit() {
        JedisCommands commands = null;
        try {
            commands = jedisContainer.getInstance();
            if (commands.exists(prepareNamespace)) {
                LOG.debug("Loading previously prepared commit from {}", prepareNamespace);
                pendingCommit = Collections.unmodifiableMap(commands.hgetAll(prepareNamespace));
            } else {
                LOG.debug("No previously prepared commits.");
                pendingCommit = Collections.emptyMap();
            }
        } finally {
            jedisContainer.returnInstance(commands);
        }
    }

    @Override
    public void put(K key, V value) {
        LOG.debug("put key '{}', value '{}'", key, value);
        pendingPrepare.put(encode(keySerializer.serialize(key)),
                           encode(valueSerializer.serialize(value)));
    }

    @Override
    public V get(K key) {
        LOG.debug("get key '{}'", key);
        String redisKey = encode(keySerializer.serialize(key));
        String redisValue = null;
        if (pendingPrepare.containsKey(redisKey)) {
            redisValue = pendingPrepare.get(redisKey);
        } else if (pendingCommit.containsKey(redisKey)) {
            redisValue = pendingCommit.get(redisKey);
        } else {
            JedisCommands commands = null;
            try {
                commands = jedisContainer.getInstance();
                redisValue = commands.hget(namespace, redisKey);
            } finally {
                jedisContainer.returnInstance(commands);
            }
        }
        V value = null;
        if (redisValue != null) {
            value = valueSerializer.deserialize(decode(redisValue));
        }
        LOG.debug("Value for key '{}' is '{}'", key, value);
        return value;
    }

    @Override
    public V get(K key, V defaultValue) {
        V val = get(key);
        return val != null ? val : defaultValue;
    }

    @Override
    public void prepareCommit(long txid) {
        LOG.debug("prepareCommit txid {}", txid);
        validatePrepareTxid(txid);
        JedisCommands commands = null;
        try {
            commands = jedisContainer.getInstance();
            if (commands.exists(prepareNamespace)) {
                LOG.debug("Prepared txn already exists, will merge", txid);
                pendingPrepare.putAll(pendingCommit);
            }
            if (!pendingPrepare.isEmpty()) {
                commands.hmset(prepareNamespace, pendingPrepare);
            } else {
                LOG.debug("Nothing to save for prepareCommit, txid {}.", txid);
            }
            txIds.put(PREPARE_TXID_KEY, String.valueOf(txid));
            commands.hmset(txidNamespace, txIds);
            pendingCommit = Collections.unmodifiableMap(pendingPrepare);
            pendingPrepare = new ConcurrentHashMap<>();
        } finally {
            jedisContainer.returnInstance(commands);
        }
    }

    @Override
    public void commit(long txid) {
        LOG.debug("commit txid {}", txid);
        validateCommitTxid(txid);
        JedisCommands commands = null;
        try {
            commands = jedisContainer.getInstance();
            if (!pendingCommit.isEmpty()) {
                commands.hmset(namespace, pendingCommit);
            } else {
                LOG.debug("Nothing to save for commit, txid {}.", txid);
            }
            txIds.put(COMMIT_TXID_KEY, String.valueOf(txid));
            commands.hmset(txidNamespace, txIds);
            commands.del(prepareNamespace);
            pendingCommit = Collections.emptyMap();
        } finally {
            jedisContainer.returnInstance(commands);
        }
    }

    @Override
    public void commit() {
        JedisCommands commands = null;
        try {
            commands = jedisContainer.getInstance();
            if (!pendingPrepare.isEmpty()) {
                commands.hmset(namespace, pendingPrepare);
            } else {
                LOG.debug("Nothing to save for commit");
            }
            pendingPrepare = new ConcurrentHashMap<>();
        } finally {
            jedisContainer.returnInstance(commands);
        }
    }

    @Override
    public void rollback() {
        LOG.debug("rollback");
        JedisCommands commands = null;
        try {
            commands = jedisContainer.getInstance();
            if (commands.exists(prepareNamespace)) {
                commands.del(prepareNamespace);
            } else {
                LOG.debug("Nothing to rollback, prepared data is empty");
            }
            Long lastCommittedId = lastCommittedTxid();
            if (lastCommittedId != null) {
                txIds.put(PREPARE_TXID_KEY, String.valueOf(lastCommittedId));
            } else {
                txIds.remove(PREPARE_TXID_KEY);
            }
            if (!txIds.isEmpty()) {
                LOG.debug("hmset txidNamespace {}, txIds {}", txidNamespace, txIds);
                commands.hmset(txidNamespace, txIds);
            }
            pendingCommit = Collections.emptyMap();
            pendingPrepare = new ConcurrentHashMap<>();
        } finally {
            jedisContainer.returnInstance(commands);
        }
    }

    /*
     * Same txid can be prepared again, but the next txid cannot be prepared
     * when previous one is not committed yet.
     */
    private void validatePrepareTxid(long txid) {
        Long committedTxid = lastCommittedTxid();
        if (committedTxid != null) {
            if (txid <= committedTxid) {
                throw new RuntimeException("Invalid txid '" + txid + "' for prepare. Txid '" + committedTxid +
                                                   "' is already committed");
            }
        }
    }

    /*
     * Same txid can be committed again but the
     * txid to be committed must be the last prepared one.
     */
    private void validateCommitTxid(long txid) {
        Long committedTxid = lastCommittedTxid();
        if (committedTxid != null) {
            if (txid < committedTxid) {
                throw new RuntimeException("Invalid txid '" + txid + "' txid '" + committedTxid + "' is already committed");
            }
        }
        Long preparedTxid = lastPreparedTxid();
        if (preparedTxid != null) {
            if (txid != preparedTxid) {
                throw new RuntimeException("Invalid txid '" + txid + "' not same as prepared txid '" + preparedTxid + "'");
            }
        }
    }

    private Long lastCommittedTxid() {
        return lastId(COMMIT_TXID_KEY);
    }

    private Long lastPreparedTxid() {
        return lastId(PREPARE_TXID_KEY);
    }

    private Long lastId(String key) {
        Long lastId = null;
        String str = txIds.get(key);
        if (str != null) {
            lastId = Long.valueOf(str);
        }
        return lastId;
    }

    private String encode(byte[] bytes) {
        return base64Encoder.encode(bytes);
    }

    private byte[] decode(String s) {
        try {
            return base64Decoder.decodeBuffer(s);
        } catch (IOException ex) {
            throw new RuntimeException("Error while decoding string " + s);
        }
    }
}
