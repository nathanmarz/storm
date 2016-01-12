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
package org.apache.storm.state;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An in-memory implementation of the {@link State}
 */
public class InMemoryKeyValueState<K, V> implements KeyValueState<K, V> {
    private static final Logger LOG = LoggerFactory.getLogger(InMemoryKeyValueState.class);
    private static final long DEFAULT_TXID = -1;
    private TxIdState<K, V> commitedState;
    private TxIdState<K, V> preparedState;
    private Map<K, V> state = new ConcurrentHashMap<>();

    private static class TxIdState<K, V> {
        private long txid;
        private Map<K, V> state;

        TxIdState(long txid, Map<K, V> state) {
            this.txid = txid;
            this.state = state;
        }

        @Override
        public String toString() {
            return "TxIdState{" +
                    "txid=" + txid +
                    ", state=" + state +
                    '}';
        }
    }

    @Override
    public void put(K key, V value) {
        state.put(key, value);
    }

    @Override
    public V get(K key) {
        return state.get(key);
    }

    @Override
    public V get(K key, V defaultValue) {
        V val = get(key);
        return val != null ? val : defaultValue;
    }

    @Override
    public void commit() {
        commitedState = new TxIdState<>(DEFAULT_TXID, new ConcurrentHashMap<>(state));
    }

    @Override
    public void prepareCommit(long txid) {
        LOG.debug("prepare commit, txid {}", txid);
        if (preparedState != null && txid > preparedState.txid) {
            throw new RuntimeException("Cannot prepare a new txn while there is a pending txn");
        }
        preparedState = new TxIdState<>(txid, new ConcurrentHashMap<K, V>(state));
    }

    @Override
    public void commit(long txid) {
        LOG.debug("commit, txid {}", txid);
        if (preparedState != null && txid == preparedState.txid) {
            commitedState = preparedState;
            preparedState = null;
        } else {
            throw new RuntimeException("Invalid prepared state for commit, " +
                                               "preparedState " + preparedState + " txid " + txid);
        }
    }

    @Override
    public void rollback() {
        preparedState = null;
        if (commitedState != null) {
            state = commitedState.state;
        } else {
            state = new ConcurrentHashMap<>();
        }
    }

    @Override
    public String toString() {
        return "InMemoryKeyValueState{" +
                "commitedState=" + commitedState +
                ", preparedState=" + preparedState +
                ", state=" + state +
                '}';
    }
}
