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

import com.google.common.collect.Lists;
import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.trident.operation.Aggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.windowing.config.WindowConfig;
import org.apache.storm.trident.windowing.strategy.WindowStrategy;
import org.apache.storm.windowing.EvictionPolicy;
import org.apache.storm.windowing.TriggerPolicy;
import org.apache.storm.windowing.WindowLifecycleListener;
import org.apache.storm.windowing.WindowManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Basic functionality to manage trident tuple events using {@code WindowManager} and {@code WindowsStore} for storing
 * tuples and triggers related information.
 *
 */
public abstract class AbstractTridentWindowManager<T> implements ITridentWindowManager {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractTridentWindowManager.class);

    protected final WindowManager<T> windowManager;
    protected final Aggregator aggregator;
    protected final BatchOutputCollector delegateCollector;
    protected final String windowTaskId;
    protected final WindowsStore windowStore;

    protected final Queue<TriggerResult> pendingTriggers = new ConcurrentLinkedQueue<>();
    protected final AtomicInteger triggerId = new AtomicInteger();
    private final String windowTriggerCountId;
    private final TriggerPolicy<T> triggerPolicy;

    public AbstractTridentWindowManager(WindowConfig windowConfig, String windowTaskId, WindowsStore windowStore,
                                        Aggregator aggregator, BatchOutputCollector delegateCollector) {
        this.windowTaskId = windowTaskId;
        this.windowStore = windowStore;
        this.aggregator = aggregator;
        this.delegateCollector = delegateCollector;

        windowTriggerCountId = WindowTridentProcessor.TRIGGER_COUNT_PREFIX + windowTaskId;

        windowManager = new WindowManager<>(new TridentWindowLifeCycleListener());

        WindowStrategy<T> windowStrategy = windowConfig.getWindowStrategy();
        EvictionPolicy<T> evictionPolicy = windowStrategy.getEvictionPolicy();
        windowManager.setEvictionPolicy(evictionPolicy);
        triggerPolicy = windowStrategy.getTriggerPolicy(windowManager, evictionPolicy);
        windowManager.setTriggerPolicy(triggerPolicy);
    }

    @Override
    public void prepare() {
        preInitialize();

        initialize();

        postInitialize();
    }

    private void preInitialize() {
        LOG.debug("Getting current trigger count for this component/task");
        // get trigger count value from store
        Object result = windowStore.get(windowTriggerCountId);
        Integer currentCount = 0;
        if(result == null) {
            LOG.info("No current trigger count in windows store.");
        } else {
            currentCount = (Integer) result + 1;
        }
        windowStore.put(windowTriggerCountId, currentCount);
        triggerId.set(currentCount);
    }

    private void postInitialize() {
        // start trigger once the initialization is done.
        triggerPolicy.start();
    }

    /**
     * Load and initialize any resources into window manager before windowing for component/task is activated.
     */
    protected abstract void initialize();

    /**
     * Listener to reeive any activation/expiry of windowing events and take further action on them.
     */
    class TridentWindowLifeCycleListener implements WindowLifecycleListener<T> {

        @Override
        public void onExpiry(List<T> expiredEvents) {
            LOG.debug("onExpiry is invoked");
            onTuplesExpired(expiredEvents);
        }

        @Override
        public void onActivation(List<T> events, List<T> newEvents, List<T> expired) {
            LOG.debug("onActivation is invoked with events size: [{}]", events.size());
            // trigger occurred, create an aggregation and keep them in store
            int currentTriggerId = triggerId.incrementAndGet();
            execAggregatorAndStoreResult(currentTriggerId, events);
        }
    }

    /**
     * Handle expired tuple events which can be removing from cache or store.
     *
     * @param expiredEvents
     */
    protected abstract void onTuplesExpired(List<T> expiredEvents);

    private void execAggregatorAndStoreResult(int currentTriggerId, List<T> tupleEvents) {
        List<TridentTuple> resultTuples = getTridentTuples(tupleEvents);

        // run aggregator to compute the result
        AccumulatedTuplesCollector collector = new AccumulatedTuplesCollector(delegateCollector);
        Object state = aggregator.init(currentTriggerId, collector);
        for (TridentTuple resultTuple : resultTuples) {
            aggregator.aggregate(state, resultTuple, collector);
        }
        aggregator.complete(state, collector);

        List<List<Object>> resultantAggregatedValue = collector.values;

        ArrayList<WindowsStore.Entry> entries = Lists.newArrayList(new WindowsStore.Entry(windowTriggerCountId, currentTriggerId + 1),
                new WindowsStore.Entry(WindowTridentProcessor.generateWindowTriggerKey(windowTaskId, currentTriggerId), resultantAggregatedValue));
        windowStore.putAll(entries);

        pendingTriggers.add(new TriggerResult(currentTriggerId, resultantAggregatedValue));
    }

    /**
     * Return {@code TridentTuple}s from given {@code tupleEvents}.
     * @param tupleEvents
     * @return
     */
    protected abstract List<TridentTuple> getTridentTuples(List<T> tupleEvents);

    /**
     * This {@code TridentCollector} accumulates all the values emitted.
     */
    static class AccumulatedTuplesCollector implements TridentCollector {

        final List<List<Object>> values = new ArrayList<>();
        private final BatchOutputCollector delegateCollector;

        public AccumulatedTuplesCollector(BatchOutputCollector delegateCollector) {
            this.delegateCollector = delegateCollector;
        }

        @Override
        public void emit(List<Object> values) {
            this.values.add(values);
        }

        @Override
        public void reportError(Throwable t) {
            delegateCollector.reportError(t);
        }

    }

    static class TriggerResult {
        final int id;
        final List<List<Object>> result;

        public TriggerResult(int id, List<List<Object>> result) {
            this.id = id;
            this.result = result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TriggerResult)) return false;

            TriggerResult that = (TriggerResult) o;

            return id == that.id;

        }

        @Override
        public int hashCode() {
            return id;
        }

        @Override
        public String toString() {
            return "TriggerResult{" +
                    "id=" + id +
                    ", result=" + result +
                    '}';
        }
    }

    public Queue<TriggerResult> getPendingTriggers() {
        return pendingTriggers;
    }

    public void shutdown() {
        try {
            LOG.info("window manager [{}] is being shutdown", windowManager);
            windowManager.shutdown();
        } finally {
            LOG.info("window store [{}] is being shutdown", windowStore);
            windowStore.shutdown();
        }
    }

}
