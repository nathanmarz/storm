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
import backtype.storm.windowing.TupleWindowImpl;
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

    private IWindowedBolt bolt;
    private transient WindowedOutputCollector windowedOutputCollector;
    private transient WindowLifecycleListener<Tuple> listener;
    private transient WindowManager<Tuple> windowManager;

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

    private WindowManager<Tuple> initWindowManager(WindowLifecycleListener<Tuple> lifecycleListener, Map stormConf) {
        WindowManager<Tuple> manager = new WindowManager<>(lifecycleListener);
        Duration windowLengthDuration = null;
        Count windowLengthCount = null;
        Duration slidingIntervalDuration = null;
        Count slidingIntervalCount = null;
        if (stormConf.containsKey(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT)) {
            windowLengthCount = new Count(((Number) stormConf.get(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT)).intValue());
        } else if (stormConf.containsKey(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS)) {
            windowLengthDuration = new Duration(
                    ((Number) stormConf.get(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS)).intValue(),
                    TimeUnit.MILLISECONDS);
        }

        if (stormConf.containsKey(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_COUNT)) {
            slidingIntervalCount = new Count(((Number) stormConf.get(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_COUNT)).intValue());
        } else if (stormConf.containsKey(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS)) {
            slidingIntervalDuration = new Duration(((Number) stormConf.get(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS)).intValue(), TimeUnit.MILLISECONDS);
        } else {
            // default is a sliding window of count 1
            slidingIntervalCount = new Count(1);
        }
        // validate
        validate(stormConf, windowLengthCount, windowLengthDuration,
                 slidingIntervalCount, slidingIntervalDuration);
        if (windowLengthCount != null) {
            manager.setWindowLength(windowLengthCount);
        } else {
            manager.setWindowLength(windowLengthDuration);
        }
        if (slidingIntervalCount != null) {
            manager.setSlidingInterval(slidingIntervalCount);
        } else {
            manager.setSlidingInterval(slidingIntervalDuration);
        }
        return manager;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.windowedOutputCollector = new WindowedOutputCollector(collector);
        bolt.prepare(stormConf, context, windowedOutputCollector);
        this.listener = newWindowLifecycleListener();
        this.windowManager = initWindowManager(listener, stormConf);
        LOG.debug("Initialized window manager {} ", this.windowManager);
    }

    @Override
    public void execute(Tuple input) {
        windowManager.add(input);
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
