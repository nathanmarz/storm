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

import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.Aggregator;
import org.apache.storm.trident.planner.ProcessorContext;
import org.apache.storm.trident.planner.TridentProcessor;
import org.apache.storm.trident.planner.processor.FreshCollector;
import org.apache.storm.trident.planner.processor.TridentContext;
import org.apache.storm.trident.spout.IBatchID;
import org.apache.storm.trident.tuple.ConsList;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.tuple.TridentTupleView;
import org.apache.storm.trident.windowing.config.WindowConfig;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * {@code TridentProcessor} implementation for windowing operations on trident stream.
 *
 */
public class WindowTridentProcessor implements TridentProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(WindowTridentProcessor.class);

    public static final String TRIGGER_INPROCESS_PREFIX = "tip" + WindowsStore.KEY_SEPARATOR;
    public static final String TRIGGER_PREFIX = "tr" + WindowsStore.KEY_SEPARATOR;
    public static final String TRIGGER_COUNT_PREFIX = "tc" + WindowsStore.KEY_SEPARATOR;

    public static final String TRIGGER_FIELD_NAME = "_task_info";
    public static final long DEFAULT_INMEMORY_TUPLE_CACHE_LIMIT = 100L;

    private final String windowId;
    private final Fields inputFields;
    private final Aggregator aggregator;
    private final boolean storeTuplesInStore;

    private String windowTriggerInprocessId;
    private WindowConfig windowConfig;
    private WindowsStoreFactory windowStoreFactory;
    private WindowsStore windowStore;

    private TopologyContext topologyContext;
    private FreshCollector collector;
    private TridentTupleView.ProjectionFactory projection;
    private TridentContext tridentContext;
    private ITridentWindowManager tridentWindowManager;
    private String windowTaskId;

    public WindowTridentProcessor(WindowConfig windowConfig, String uniqueWindowId, WindowsStoreFactory windowStoreFactory,
                                  Fields inputFields, Aggregator aggregator, boolean storeTuplesInStore) {

        this.windowConfig = windowConfig;
        this.windowId = uniqueWindowId;
        this.windowStoreFactory = windowStoreFactory;
        this.inputFields = inputFields;
        this.aggregator = aggregator;
        this.storeTuplesInStore = storeTuplesInStore;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, TridentContext tridentContext) {
        this.topologyContext = context;
        List<TridentTuple.Factory> parents = tridentContext.getParentTupleFactories();
        if (parents.size() != 1) {
            throw new RuntimeException("Aggregation related operation can only have one parent");
        }

        Long maxTuplesCacheSize = getWindowTuplesCacheSize(stormConf);

        this.tridentContext = tridentContext;
        collector = new FreshCollector(tridentContext);
        projection = new TridentTupleView.ProjectionFactory(parents.get(0), inputFields);

        windowStore = windowStoreFactory.create(stormConf);
        windowTaskId = windowId + WindowsStore.KEY_SEPARATOR + topologyContext.getThisTaskId() + WindowsStore.KEY_SEPARATOR;
        windowTriggerInprocessId = getWindowTriggerInprocessIdPrefix(windowTaskId);

        tridentWindowManager = storeTuplesInStore ?
                new StoreBasedTridentWindowManager(windowConfig, windowTaskId, windowStore, aggregator, tridentContext.getDelegateCollector(), maxTuplesCacheSize, inputFields)
                : new InMemoryTridentWindowManager(windowConfig, windowTaskId, windowStore, aggregator, tridentContext.getDelegateCollector());

        tridentWindowManager.prepare();
    }

    public static String getWindowTriggerInprocessIdPrefix(String windowTaskId) {
        return TRIGGER_INPROCESS_PREFIX + windowTaskId;
    }

    public static String getWindowTriggerTaskPrefix(String windowTaskId) {
        return TRIGGER_PREFIX + windowTaskId;
    }

    private Long getWindowTuplesCacheSize(Map conf) {
        if (conf.containsKey(Config.TOPOLOGY_TRIDENT_WINDOWING_INMEMORY_CACHE_LIMIT)) {
            return ((Number) conf.get(Config.TOPOLOGY_TRIDENT_WINDOWING_INMEMORY_CACHE_LIMIT)).longValue();
        }
        return DEFAULT_INMEMORY_TUPLE_CACHE_LIMIT;
    }

    @Override
    public void cleanup() {
        LOG.info("shutting down window manager");
        try {
            tridentWindowManager.shutdown();
        } catch (Exception ex) {
            LOG.error("Error occurred while cleaning up window processor", ex);
            throw ex;
        }
    }

    @Override
    public void startBatch(ProcessorContext processorContext) {
        // initialize state for batch
        processorContext.state[tridentContext.getStateIndex()] = new ArrayList<TridentTuple>();
    }

    @Override
    public void execute(ProcessorContext processorContext, String streamId, TridentTuple tuple) {
        // add tuple to the batch state
        Object state = processorContext.state[tridentContext.getStateIndex()];
        ((List<TridentTuple>) state).add(projection.create(tuple));
    }

    @Override
    public void finishBatch(ProcessorContext processorContext) {

        Object batchId = processorContext.batchId;
        Object batchTxnId = getBatchTxnId(batchId);

        LOG.debug("Received finishBatch of : [{}] ", batchId);
        // get all the tuples in a batch and add it to trident-window-manager
        List<TridentTuple> tuples = (List<TridentTuple>) processorContext.state[tridentContext.getStateIndex()];
        tridentWindowManager.addTuplesBatch(batchId, tuples);

        List<Integer> pendingTriggerIds = null;
        List<String> triggerKeys = new ArrayList<>();
        Iterable<Object> triggerValues = null;

        if (retriedAttempt(batchId)) {
            pendingTriggerIds = (List<Integer>) windowStore.get(inprocessTriggerKey(batchTxnId));
            for (Integer pendingTriggerId : pendingTriggerIds) {
                triggerKeys.add(triggerKey(pendingTriggerId));
            }
            triggerValues = windowStore.get(triggerKeys);
        }

        // if there are no trigger values in earlier attempts or this is a new batch, emit pending triggers.
        if(triggerValues == null) {
            pendingTriggerIds = new ArrayList<>();
            Queue<StoreBasedTridentWindowManager.TriggerResult> pendingTriggers = tridentWindowManager.getPendingTriggers();
            LOG.debug("pending triggers at batch: [{}] and triggers.size: [{}] ", batchId, pendingTriggers.size());
            try {
                Iterator<StoreBasedTridentWindowManager.TriggerResult> pendingTriggersIter = pendingTriggers.iterator();
                List<Object> values = new ArrayList<>();
                StoreBasedTridentWindowManager.TriggerResult triggerResult = null;
                while (pendingTriggersIter.hasNext()) {
                    triggerResult = pendingTriggersIter.next();
                    for (List<Object> aggregatedResult : triggerResult.result) {
                        String triggerKey = triggerKey(triggerResult.id);
                        triggerKeys.add(triggerKey);
                        values.add(aggregatedResult);
                        pendingTriggerIds.add(triggerResult.id);
                    }
                    pendingTriggersIter.remove();
                }
                triggerValues = values;
            } finally {
                // store inprocess triggers of a batch in store for batch retries for any failures
                if (!pendingTriggerIds.isEmpty()) {
                    windowStore.put(inprocessTriggerKey(batchTxnId), pendingTriggerIds);
                }
            }
        }

        collector.setContext(processorContext);
        int i = 0;
        for (Object resultValue : triggerValues) {
            collector.emit(new ConsList(new TriggerInfo(windowTaskId, pendingTriggerIds.get(i++)), (List<Object>) resultValue));
        }
        collector.setContext(null);
    }

    private String inprocessTriggerKey(Object batchTxnId) {
        return windowTriggerInprocessId + batchTxnId;
    }

    public static Object getBatchTxnId(Object batchId) {
        if (batchId instanceof IBatchID) {
            return ((IBatchID) batchId).getId();
        }
        return null;
    }

    static boolean retriedAttempt(Object batchId) {
        if (batchId instanceof IBatchID) {
            return ((IBatchID) batchId).getAttemptId() > 0;
        }

        return false;
    }

    @Override
    public TridentTuple.Factory getOutputFactory() {
        return collector.getOutputFactory();
    }

    public static class TriggerInfo implements Serializable {
        public final String windowTaskId;
        public final int triggerId;

        public TriggerInfo(String windowTaskId, int triggerId) {
            this.windowTaskId = windowTaskId;
            this.triggerId = triggerId;
        }

        public String generateTriggerKey() {
            return generateWindowTriggerKey(windowTaskId, triggerId);
        }

        @Override
        public String toString() {
            return "TriggerInfo{" +
                    "windowTaskId='" + windowTaskId + '\'' +
                    ", triggerId=" + triggerId +
                    '}';
        }
    }

    public String triggerKey(int triggerId) {
        return generateWindowTriggerKey(windowTaskId, triggerId);
    }

    public static String generateWindowTriggerKey(String windowTaskId, int triggerId) {
        return TRIGGER_PREFIX + windowTaskId + triggerId;
    }

}
