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
package backtype.storm.topology;

import backtype.storm.Config;
import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.windowing.CountEvictionPolicy;
import backtype.storm.windowing.CountTriggerPolicy;
import backtype.storm.windowing.EvictionPolicy;
import backtype.storm.windowing.TimeEvictionPolicy;
import backtype.storm.windowing.TimeTriggerPolicy;
import backtype.storm.windowing.TriggerPolicy;
import backtype.storm.windowing.TupleWindowImpl;
import backtype.storm.windowing.WaterMarkEventGenerator;
import backtype.storm.windowing.WatermarkCountEvictionPolicy;
import backtype.storm.windowing.WatermarkCountTriggerPolicy;
import backtype.storm.windowing.WatermarkTimeEvictionPolicy;
import backtype.storm.windowing.WatermarkTimeTriggerPolicy;
import backtype.storm.windowing.WindowLifecycleListener;
import backtype.storm.windowing.WindowManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static backtype.storm.topology.base.BaseWindowedBolt.Count;
import static backtype.storm.topology.base.BaseWindowedBolt.Duration;

/**
 * An {@link IWindowedBolt} wrapper that does the windowing of tuples.
 */
public class WindowedBoltExecutor implements IRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(WindowedBoltExecutor.class);
    private static final int DEFAULT_WATERMARK_EVENT_INTERVAL_MS = 1000; // 1s
    private static final int DEFAULT_MAX_LAG_MS = 0; // no lag
    private final IWindowedBolt bolt;
    private transient WindowedOutputCollector windowedOutputCollector;
    private transient WindowLifecycleListener<Tuple> listener;
    private transient WindowManager<Tuple> windowManager;
    private transient int maxLagMs;
    private transient String tupleTsFieldName;
    // package level for unit tests
    transient WaterMarkEventGenerator<Tuple> waterMarkEventGenerator;

    public WindowedBoltExecutor(IWindowedBolt bolt) {
        this.bolt = bolt;
    }

    private int getTopologyTimeoutMillis(Map stormConf) {
        if (stormConf.get(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS) != null) {
            boolean timeOutsEnabled = (boolean) stormConf.get(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS);
            if (!timeOutsEnabled) {
                return Integer.MAX_VALUE;
            }
        }
        int timeout = 0;
        if (stormConf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS) != null) {
            timeout = ((Number) stormConf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS)).intValue();
        }
        return timeout * 1000;
    }

    private int getMaxSpoutPending(Map stormConf) {
        int maxPending = Integer.MAX_VALUE;
        if (stormConf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING) != null) {
            maxPending = ((Number) stormConf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING)).intValue();
        }
        return maxPending;
    }

    private void ensureDurationLessThanTimeout(int duration, int timeout) {
        if (duration > timeout) {
            throw new IllegalArgumentException("Window duration (length + sliding interval) value " + duration +
                                                       " is more than " + Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS +
                                                       " value " + timeout);
        }
    }

    private void ensureCountLessThanMaxPending(int count, int maxPending) {
        if (count > maxPending) {
            throw new IllegalArgumentException("Window count (length + sliding interval) value " + count +
                                                       " is more than " + Config.TOPOLOGY_MAX_SPOUT_PENDING +
                                                       " value " + maxPending);
        }
    }

    private void validate(Map stormConf, Count windowLengthCount, Duration windowLengthDuration,
                          Count slidingIntervalCount, Duration slidingIntervalDuration) {

        int topologyTimeout = getTopologyTimeoutMillis(stormConf);
        int maxSpoutPending = getMaxSpoutPending(stormConf);
        if (windowLengthCount == null && windowLengthDuration == null) {
            throw new IllegalArgumentException("Window length is not specified");
        }

        if (windowLengthDuration != null && slidingIntervalDuration != null) {
            ensureDurationLessThanTimeout(windowLengthDuration.value + slidingIntervalDuration.value, topologyTimeout);
        } else if (windowLengthDuration != null) {
            ensureDurationLessThanTimeout(windowLengthDuration.value, topologyTimeout);
        } else if (slidingIntervalDuration != null) {
            ensureDurationLessThanTimeout(slidingIntervalDuration.value, topologyTimeout);
        }

        if (windowLengthCount != null && slidingIntervalCount != null) {
            ensureCountLessThanMaxPending(windowLengthCount.value + slidingIntervalCount.value, maxSpoutPending);
        } else if (windowLengthCount != null) {
            ensureCountLessThanMaxPending(windowLengthCount.value, maxSpoutPending);
        } else if (slidingIntervalCount != null) {
            ensureCountLessThanMaxPending(slidingIntervalCount.value, maxSpoutPending);
        }
    }

    private WindowManager<Tuple> initWindowManager(WindowLifecycleListener<Tuple> lifecycleListener, Map stormConf,
                                                   TopologyContext context) {
        WindowManager<Tuple> manager = new WindowManager<>(lifecycleListener);
        Duration windowLengthDuration = null;
        Count windowLengthCount = null;
        Duration slidingIntervalDuration = null;
        Count slidingIntervalCount = null;
        // window length
        if (stormConf.containsKey(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT)) {
            windowLengthCount = new Count(((Number) stormConf.get(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT)).intValue());
        } else if (stormConf.containsKey(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS)) {
            windowLengthDuration = new Duration(
                    ((Number) stormConf.get(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS)).intValue(),
                    TimeUnit.MILLISECONDS);
        }
        // sliding interval
        if (stormConf.containsKey(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_COUNT)) {
            slidingIntervalCount = new Count(((Number) stormConf.get(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_COUNT)).intValue());
        } else if (stormConf.containsKey(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS)) {
            slidingIntervalDuration = new Duration(((Number) stormConf.get(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS)).intValue(), TimeUnit.MILLISECONDS);
        } else {
            // default is a sliding window of count 1
            slidingIntervalCount = new Count(1);
        }
        // tuple ts
        if (stormConf.containsKey(Config.TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_FIELD_NAME)) {
            tupleTsFieldName = (String) stormConf.get(Config.TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_FIELD_NAME);
            // max lag
            if (stormConf.containsKey(Config.TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_MAX_LAG_MS)) {
                maxLagMs = ((Number) stormConf.get(Config.TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_MAX_LAG_MS)).intValue();
            } else {
                maxLagMs = DEFAULT_MAX_LAG_MS;
            }
            // watermark interval
            int watermarkInterval;
            if (stormConf.containsKey(Config.TOPOLOGY_BOLTS_WATERMARK_EVENT_INTERVAL_MS)) {
                watermarkInterval = ((Number) stormConf.get(Config.TOPOLOGY_BOLTS_WATERMARK_EVENT_INTERVAL_MS)).intValue();
            } else {
                watermarkInterval = DEFAULT_WATERMARK_EVENT_INTERVAL_MS;
            }
            waterMarkEventGenerator = new WaterMarkEventGenerator<>(manager, watermarkInterval,
                                                                    maxLagMs, context.getThisSources().keySet());
        }
        // validate
        validate(stormConf, windowLengthCount, windowLengthDuration,
                 slidingIntervalCount, slidingIntervalDuration);
        EvictionPolicy<Tuple> evictionPolicy = getEvictionPolicy(windowLengthCount, windowLengthDuration,
                                                                 manager);
        TriggerPolicy<Tuple> triggerPolicy = getTriggerPolicy(slidingIntervalCount, slidingIntervalDuration,
                                                              manager, evictionPolicy);
        manager.setEvictionPolicy(evictionPolicy);
        manager.setTriggerPolicy(triggerPolicy);
        return manager;
    }

    private boolean isTupleTs() {
        return tupleTsFieldName != null;
    }

    private TriggerPolicy<Tuple> getTriggerPolicy(Count slidingIntervalCount, Duration slidingIntervalDuration,
                                                  WindowManager<Tuple> manager, EvictionPolicy<Tuple> evictionPolicy) {
        if (slidingIntervalCount != null) {
            if (isTupleTs()) {
                return new WatermarkCountTriggerPolicy<>(slidingIntervalCount.value, manager, evictionPolicy, manager);
            } else {
                return new CountTriggerPolicy<>(slidingIntervalCount.value, manager, evictionPolicy);
            }
        } else {
            if (isTupleTs()) {
                return new WatermarkTimeTriggerPolicy<>(slidingIntervalDuration.value, manager, evictionPolicy, manager);
            } else {
                return new TimeTriggerPolicy<>(slidingIntervalDuration.value, manager, evictionPolicy);
            }
        }
    }

    private EvictionPolicy<Tuple> getEvictionPolicy(Count windowLengthCount, Duration windowLengthDuration,
                                                    WindowManager<Tuple> manager) {
        if (windowLengthCount != null) {
            if (isTupleTs()) {
                return new WatermarkCountEvictionPolicy<>(windowLengthCount.value, manager);
            } else {
                return new CountEvictionPolicy<>(windowLengthCount.value);
            }
        } else {
            if (isTupleTs()) {
                return new WatermarkTimeEvictionPolicy<>(windowLengthDuration.value, maxLagMs);
            } else {
                return new TimeEvictionPolicy<>(windowLengthDuration.value);
            }
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.windowedOutputCollector = new WindowedOutputCollector(collector);
        bolt.prepare(stormConf, context, windowedOutputCollector);
        this.listener = newWindowLifecycleListener();
        this.windowManager = initWindowManager(listener, stormConf, context);
        LOG.debug("Initialized window manager {} ", this.windowManager);
    }

    @Override
    public void execute(Tuple input) {
        if (isTupleTs()) {
            long ts = input.getLongByField(tupleTsFieldName);
            if (waterMarkEventGenerator.track(input.getSourceGlobalStreamId(), ts)) {
                windowManager.add(input, ts);
            } else {
                LOG.info("Received a late tuple {} with ts {}. This will not processed.", input, ts);
            }
        } else {
            windowManager.add(input);
        }
    }

    @Override
    public void cleanup() {
        windowManager.shutdown();
        bolt.cleanup();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        bolt.declareOutputFields(declarer);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return bolt.getComponentConfiguration();
    }

    private WindowLifecycleListener<Tuple> newWindowLifecycleListener() {
        return new WindowLifecycleListener<Tuple>() {
            @Override
            public void onExpiry(List<Tuple> tuples) {
                for (Tuple tuple : tuples) {
                    windowedOutputCollector.ack(tuple);
                }
            }

            @Override
            public void onActivation(List<Tuple> tuples, List<Tuple> newTuples, List<Tuple> expiredTuples) {
                windowedOutputCollector.setContext(tuples);
                bolt.execute(new TupleWindowImpl(tuples, newTuples, expiredTuples));
            }
        };
    }

    /**
     * Creates an {@link OutputCollector} wrapper that automatically
     * anchors the tuples to inputTuples while emitting.
     */
    private static class WindowedOutputCollector extends OutputCollector {
        private List<Tuple> inputTuples;

        WindowedOutputCollector(IOutputCollector delegate) {
            super(delegate);
        }

        void setContext(List<Tuple> inputTuples) {
            this.inputTuples = inputTuples;
        }

        @Override
        public List<Integer> emit(String streamId, List<Object> tuple) {
            return emit(streamId, inputTuples, tuple);
        }

        @Override
        public void emitDirect(int taskId, String streamId, List<Object> tuple) {
            emitDirect(taskId, streamId, inputTuples, tuple);
        }
    }

}
