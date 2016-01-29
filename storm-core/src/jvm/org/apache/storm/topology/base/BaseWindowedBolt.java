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
package org.apache.storm.topology.base;

import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IWindowedBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public abstract class BaseWindowedBolt implements IWindowedBolt {
    private static final Logger LOG = LoggerFactory.getLogger(BaseWindowedBolt.class);

    protected final transient Map<String, Object> windowConfiguration;

    /**
     * Holds a count value for count based windows and sliding intervals.
     */
    public static class Count {
        public final int value;

        public Count(int value) {
            this.value = value;
        }
    }

    /**
     * Holds a Time duration for time based windows and sliding intervals.
     */
    public static class Duration {
        public final int value;

        public Duration(int value, TimeUnit timeUnit) {
            this.value = (int) timeUnit.toMillis(value);
        }
    }

    protected BaseWindowedBolt() {
        windowConfiguration = new HashMap<>();
    }

    private BaseWindowedBolt withWindowLength(Count count) {
        windowConfiguration.put(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT, count.value);
        return this;
    }

    private BaseWindowedBolt withWindowLength(Duration duration) {
        windowConfiguration.put(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS, duration.value);
        return this;
    }

    private BaseWindowedBolt withSlidingInterval(Count count) {
        windowConfiguration.put(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_COUNT, count.value);
        return this;
    }

    private BaseWindowedBolt withSlidingInterval(Duration duration) {
        windowConfiguration.put(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS, duration.value);
        return this;
    }

    /**
     * Tuple count based sliding window configuration.
     *
     * @param windowLength    the number of tuples in the window
     * @param slidingInterval the number of tuples after which the window slides
     */
    public BaseWindowedBolt withWindow(Count windowLength, Count slidingInterval) {
        return withWindowLength(windowLength).withSlidingInterval(slidingInterval);
    }

    /**
     * Tuple count and time duration based sliding window configuration.
     *
     * @param windowLength    the number of tuples in the window
     * @param slidingInterval the time duration after which the window slides
     */
    public BaseWindowedBolt withWindow(Count windowLength, Duration slidingInterval) {
        return withWindowLength(windowLength).withSlidingInterval(slidingInterval);
    }

    /**
     * Time duration and count based sliding window configuration.
     *
     * @param windowLength    the time duration of the window
     * @param slidingInterval the number of tuples after which the window slides
     */
    public BaseWindowedBolt withWindow(Duration windowLength, Count slidingInterval) {
        return withWindowLength(windowLength).withSlidingInterval(slidingInterval);
    }

    /**
     * Time duration based sliding window configuration.
     *
     * @param windowLength    the time duration of the window
     * @param slidingInterval the time duration after which the window slides
     */
    public BaseWindowedBolt withWindow(Duration windowLength, Duration slidingInterval) {
        return withWindowLength(windowLength).withSlidingInterval(slidingInterval);
    }

    /**
     * A tuple count based window that slides with every incoming tuple.
     *
     * @param windowLength the number of tuples in the window
     */
    public BaseWindowedBolt withWindow(Count windowLength) {
        return withWindowLength(windowLength).withSlidingInterval(new Count(1));
    }

    /**
     * A time duration based window that slides with every incoming tuple.
     *
     * @param windowLength the time duration of the window
     */
    public BaseWindowedBolt withWindow(Duration windowLength) {
        return withWindowLength(windowLength).withSlidingInterval(new Count(1));
    }

    /**
     * A count based tumbling window.
     *
     * @param count the number of tuples after which the window tumbles
     */
    public BaseWindowedBolt withTumblingWindow(Count count) {
        return withWindowLength(count).withSlidingInterval(count);
    }

    /**
     * A time duration based tumbling window.
     *
     * @param duration the time duration after which the window tumbles
     */
    public BaseWindowedBolt withTumblingWindow(Duration duration) {
        return withWindowLength(duration).withSlidingInterval(duration);
    }

    /**
     * Specify a field in the tuple that represents the timestamp as a long value. If this
     * field is not present in the incoming tuple, an {@link IllegalArgumentException} will be thrown.
     *
     * @param fieldName the name of the field that contains the timestamp
     */
    public BaseWindowedBolt withTimestampField(String fieldName) {
        windowConfiguration.put(Config.TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_FIELD_NAME, fieldName);
        return this;
    }

    /**
     * Specify the maximum time lag of the tuple timestamp in milliseconds. It means that the tuple timestamps
     * cannot be out of order by more than this amount.
     *
     * @param duration the max lag duration
     */
    public BaseWindowedBolt withLag(Duration duration) {
        windowConfiguration.put(Config.TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_MAX_LAG_MS, duration.value);
        return this;
    }

    /**
     * Specify the watermark event generation interval. For tuple based timestamps, watermark events
     * are used to track the progress of time
     *
     * @param interval the interval at which watermark events are generated
     */
    public BaseWindowedBolt withWatermarkInterval(Duration interval) {
        windowConfiguration.put(Config.TOPOLOGY_BOLTS_WATERMARK_EVENT_INTERVAL_MS, interval.value);
        return this;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        // NOOP
    }

    @Override
    public void cleanup() {
        // NOOP
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // NOOP
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return windowConfiguration;
    }
}
