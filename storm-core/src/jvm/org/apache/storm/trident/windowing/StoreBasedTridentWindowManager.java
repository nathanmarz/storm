/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.trident.windowing;

import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.trident.operation.Aggregator;
import org.apache.storm.trident.spout.IBatchID;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.tuple.TridentTupleView;
import org.apache.storm.trident.windowing.config.WindowConfig;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This window manager uses {@code WindowsStore} for storing tuples and other trigger related information. It maintains
 * tuples cache of {@code maxCachedTuplesSize} without accessing store for getting them.
 *
 */
public class StoreBasedTridentWindowManager extends AbstractTridentWindowManager<TridentBatchTuple> {
    private static final Logger LOG = LoggerFactory.getLogger(StoreBasedTridentWindowManager.class);

    private static final String TUPLE_PREFIX = "tu" + WindowsStore.KEY_SEPARATOR;

    private final String windowTupleTaskId;
    private final TridentTupleView.FreshOutputFactory freshOutputFactory;

    private Long maxCachedTuplesSize;
    private final Fields inputFields;
    private AtomicLong currentCachedTuplesSize = new AtomicLong();

    public StoreBasedTridentWindowManager(WindowConfig windowConfig, String windowTaskId, WindowsStore windowStore, Aggregator aggregator,
                                          BatchOutputCollector delegateCollector, Long maxTuplesCacheSize, Fields inputFields) {
        super(windowConfig, windowTaskId, windowStore, aggregator, delegateCollector);

        this.maxCachedTuplesSize = maxTuplesCacheSize;
        this.inputFields = inputFields;
        freshOutputFactory = new TridentTupleView.FreshOutputFactory(inputFields);
        windowTupleTaskId = TUPLE_PREFIX + windowTaskId;
    }

    protected void initialize() {

        // get existing tuples and pending/unsuccessful triggers for this operator-component/task and add them to WindowManager
        String windowTriggerInprocessId = WindowTridentProcessor.getWindowTriggerInprocessIdPrefix(windowTaskId);
        String windowTriggerTaskId = WindowTridentProcessor.getWindowTriggerTaskPrefix(windowTaskId);

        List<String> attemptedTriggerKeys = new ArrayList<>();
        List<String> triggerKeys = new ArrayList<>();

        Iterable<String> allEntriesIterable = windowStore.getAllKeys();
        for (String key : allEntriesIterable) {
            if (key.startsWith(windowTupleTaskId)) {
                int tupleIndexValue = lastPart(key);
                String batchId = secondLastPart(key);
                LOG.debug("Received tuple with batch [{}] and tuple index [{}]", batchId, tupleIndexValue);
                windowManager.add(new TridentBatchTuple(batchId, System.currentTimeMillis(), tupleIndexValue));
            } else if (key.startsWith(windowTriggerTaskId)) {
                triggerKeys.add(key);
                LOG.debug("Received trigger with key [{}]", key);
            } else if(key.startsWith(windowTriggerInprocessId)) {
                attemptedTriggerKeys.add(key);
                LOG.debug("Received earlier unsuccessful trigger [{}] from windows store [{}]", key);
            }
        }

        // these triggers will be retried as part of batch retries
        Set<Integer> triggersToBeIgnored = new HashSet<>();
        Iterable<Object> attemptedTriggers = windowStore.get(attemptedTriggerKeys);
        for (Object attemptedTrigger : attemptedTriggers) {
            triggersToBeIgnored.addAll((List<Integer>) attemptedTrigger);
        }

        // get trigger values only if they have more than zero
        Iterable<Object> triggerObjects = windowStore.get(triggerKeys);
        int i=0;
        for (Object triggerObject : triggerObjects) {
            int id = lastPart(triggerKeys.get(i++));
            if(!triggersToBeIgnored.contains(id)) {
                LOG.info("Adding pending trigger value [{}]", triggerObject);
                pendingTriggers.add(new TriggerResult(id, (List<List<Object>>) triggerObject));
            }
        }

    }

    private int lastPart(String key) {
        int lastSepIndex = key.lastIndexOf(WindowsStore.KEY_SEPARATOR);
        if (lastSepIndex < 0) {
            throw new IllegalArgumentException("primaryKey does not have key separator '" + WindowsStore.KEY_SEPARATOR + "'");
        }
        return Integer.parseInt(key.substring(lastSepIndex+1));
    }

    private String secondLastPart(String key) {
        int lastSepIndex = key.lastIndexOf(WindowsStore.KEY_SEPARATOR);
        if (lastSepIndex < 0) {
            throw new IllegalArgumentException("key "+key+" does not have key separator '" + WindowsStore.KEY_SEPARATOR + "'");
        }
        String trimKey = key.substring(0, lastSepIndex);
        int secondLastSepIndex = trimKey.lastIndexOf(WindowsStore.KEY_SEPARATOR);
        if (lastSepIndex < 0) {
            throw new IllegalArgumentException("key "+key+" does not have second key separator '" + WindowsStore.KEY_SEPARATOR + "'");
        }

        return key.substring(secondLastSepIndex+1, lastSepIndex);
    }

    public void addTuplesBatch(Object batchId, List<TridentTuple> tuples) {
        LOG.debug("Adding tuples to window-manager for batch: [{}]", batchId);
        List<WindowsStore.Entry> entries = new ArrayList<>();
        for (int i = 0; i < tuples.size(); i++) {
            String key = keyOf(batchId);
            TridentTuple tridentTuple = tuples.get(i);
            entries.add(new WindowsStore.Entry(key+i, tridentTuple.select(inputFields)));
        }

        // tuples should be available in store before they are added to window manager
        windowStore.putAll(entries);

        for (int i = 0; i < tuples.size(); i++) {
            String key = keyOf(batchId);
            TridentTuple tridentTuple = tuples.get(i);
            addToWindowManager(i, key, tridentTuple);
        }

    }

    private void addToWindowManager(int tupleIndex, String effectiveBatchId, TridentTuple tridentTuple) {
        TridentTuple actualTuple = null;
        if (maxCachedTuplesSize == null || currentCachedTuplesSize.get() < maxCachedTuplesSize) {
            actualTuple = tridentTuple;
        }
        currentCachedTuplesSize.incrementAndGet();
        windowManager.add(new TridentBatchTuple(effectiveBatchId, System.currentTimeMillis(), tupleIndex, actualTuple));
    }

    public String getBatchTxnId(Object batchId) {
        if (!(batchId instanceof IBatchID)) {
            throw new IllegalArgumentException("argument should be an IBatchId instance");
        }
        return ((IBatchID) batchId).getId().toString();
    }

    public String keyOf(Object batchId) {
        return windowTupleTaskId + getBatchTxnId(batchId) + WindowsStore.KEY_SEPARATOR;
    }

    public List<TridentTuple> getTridentTuples(List<TridentBatchTuple> tridentBatchTuples) {
        List<TridentTuple> resultTuples = new ArrayList<>();
        List<String> keys = new ArrayList<>();
        for (TridentBatchTuple tridentBatchTuple : tridentBatchTuples) {
            TridentTuple tuple = collectTridentTupleOrKey(tridentBatchTuple, keys);
            if(tuple != null) {
                resultTuples.add(tuple);
            }
        }

        if(keys.size() > 0) {
            Iterable<Object> storedTupleValues = windowStore.get(keys);
            for (Object storedTupleValue : storedTupleValues) {
                TridentTuple tridentTuple = freshOutputFactory.create((List<Object>) storedTupleValue);
                resultTuples.add(tridentTuple);
            }
        }

        return resultTuples;
    }

    public TridentTuple collectTridentTupleOrKey(TridentBatchTuple tridentBatchTuple, List<String> keys) {
        if (tridentBatchTuple.tridentTuple != null) {
            return tridentBatchTuple.tridentTuple;
        }
        keys.add(tupleKey(tridentBatchTuple));
        return null;
    }

    public void onTuplesExpired(List<TridentBatchTuple> expiredTuples) {
        if (maxCachedTuplesSize != null) {
            currentCachedTuplesSize.addAndGet(-expiredTuples.size());
        }

        List<String> keys = new ArrayList<>();
        for (TridentBatchTuple expiredTuple : expiredTuples) {
            keys.add(tupleKey(expiredTuple));
        }

        windowStore.removeAll(keys);
    }

    private String tupleKey(TridentBatchTuple tridentBatchTuple) {
        return tridentBatchTuple.effectiveBatchId + tridentBatchTuple.tupleIndex;
    }

}
