/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.storm.kafka.spout;

import org.apache.kafka.common.serialization.Deserializer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * KafkaSpoutConfig defines the required configuration to connect a consumer to a consumer group, as well as the subscribing topics
 */
public class KafkaSpoutConfig<K, V> implements Serializable {
    public static final long DEFAULT_POLL_TIMEOUT_MS = 2_000;            // 2s
    public static final long DEFAULT_OFFSET_COMMIT_PERIOD_MS = 15_000;   // 15s
    public static final int DEFAULT_MAX_RETRIES = Integer.MAX_VALUE;     // Retry forever
    public static final int DEFAULT_MAX_UNCOMMITTED_OFFSETS = 10_000;    // 10,000 records

    // Kafka property names
    public interface Consumer {
        String GROUP_ID = "group.id";
        String BOOTSTRAP_SERVERS = "bootstrap.servers";
        String ENABLE_AUTO_COMMIT = "enable.auto.commit";
        String AUTO_COMMIT_INTERVAL_MS = "auto.commit.interval.ms";
        String KEY_DESERIALIZER = "key.deserializer";
        String VALUE_DESERIALIZER = "value.deserializer";
    }

    /**
     * The offset used by the Kafka spout in the first poll to Kafka broker. The choice of this parameter will
     * affect the number of consumer records returned in the first poll. By default this parameter is set to UNCOMMITTED_EARLIEST. <br/><br/>
     * The allowed values are EARLIEST, LATEST, UNCOMMITTED_EARLIEST, UNCOMMITTED_LATEST. <br/>
     * <ul>
     * <li>EARLIEST means that the kafka spout polls records starting in the first offset of the partition, regardless of previous commits</li>
     * <li>LATEST means that the kafka spout polls records with offsets greater than the last offset in the partition, regardless of previous commits</li>
     * <li>UNCOMMITTED_EARLIEST means that the kafka spout polls records from the last committed offset, if any.
     * If no offset has been committed, it behaves as EARLIEST.</li>
     * <li>UNCOMMITTED_LATEST means that the kafka spout polls records from the last committed offset, if any.
     * If no offset has been committed, it behaves as LATEST.</li>
     * </ul>
     * */
    public enum FirstPollOffsetStrategy {
        EARLIEST,
        LATEST,
        UNCOMMITTED_EARLIEST,
        UNCOMMITTED_LATEST }

    /**
     * Defines when to poll the next batch of records from Kafka. The choice of this parameter will affect throughput and the memory
     * footprint of the Kafka spout. The allowed values are STREAM and BATCH. STREAM will likely have higher throughput and use more memory
     * (it stores in memory the entire KafkaRecord,including data). BATCH will likely have less throughput but also use less memory.
     * The BATCH behavior is similar to the behavior of the previous Kafka Spout. De default value is STREAM.
     * <ul>
     *     <li>STREAM Every periodic call to nextTuple polls a new batch of records from Kafka as long as the maxUncommittedOffsets
     *     threshold has not yet been reached. When the threshold his reached, no more records are polled until enough offsets have been
     *     committed, such that the number of pending offsets is less than maxUncommittedOffsets. See {@link Builder#setMaxUncommittedOffsets(int)}
     *     </li>
     *     <li>BATCH Only polls a new batch of records from kafka once all the records that came in the previous poll have been acked.</li>
     * </ul>
     */
    public enum PollStrategy {
        STREAM,
        BATCH
    }

    // Kafka consumer configuration
    private final Map<String, Object> kafkaProps;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;
    private final long pollTimeoutMs;

    // Kafka spout configuration
    private final long offsetCommitPeriodMs;
    private final int maxRetries;
    private final int maxUncommittedOffsets;
    private final FirstPollOffsetStrategy firstPollOffsetStrategy;
    private final PollStrategy pollStrategy;
    private final KafkaSpoutStreams kafkaSpoutStreams;

    private KafkaSpoutConfig(Builder<K,V> builder) {
        this.kafkaProps = setDefaultsAndGetKafkaProps(builder.kafkaProps);
        this.keyDeserializer = builder.keyDeserializer;
        this.valueDeserializer = builder.valueDeserializer;
        this.pollTimeoutMs = builder.pollTimeoutMs;
        this.offsetCommitPeriodMs = builder.offsetCommitPeriodMs;
        this.maxRetries = builder.maxRetries;
        this.firstPollOffsetStrategy = builder.firstPollOffsetStrategy;
        this.pollStrategy = builder.pollStrategy;
        this.kafkaSpoutStreams = builder.kafkaSpoutStreams;
        this.maxUncommittedOffsets = builder.maxUncommittedOffsets;
    }

    private Map<String, Object> setDefaultsAndGetKafkaProps(Map<String, Object> kafkaProps) {
        // set defaults for properties not specified
        if (!kafkaProps.containsKey(Consumer.ENABLE_AUTO_COMMIT)) {
            kafkaProps.put(Consumer.ENABLE_AUTO_COMMIT, "false");
        }
        return kafkaProps;
    }

    public static class Builder<K,V> {
        private Map<String, Object> kafkaProps;
        private Deserializer<K> keyDeserializer;
        private Deserializer<V> valueDeserializer;
        private long pollTimeoutMs = DEFAULT_POLL_TIMEOUT_MS;
        private long offsetCommitPeriodMs = DEFAULT_OFFSET_COMMIT_PERIOD_MS;
        private int maxRetries = DEFAULT_MAX_RETRIES;
        private FirstPollOffsetStrategy firstPollOffsetStrategy = FirstPollOffsetStrategy.UNCOMMITTED_EARLIEST;
        private KafkaSpoutStreams kafkaSpoutStreams;
        private int maxUncommittedOffsets = DEFAULT_MAX_UNCOMMITTED_OFFSETS;
        private PollStrategy pollStrategy = PollStrategy.STREAM;

        /***
         * KafkaSpoutConfig defines the required configuration to connect a consumer to a consumer group, as well as the subscribing topics
         * The optional configuration can be specified using the set methods of this builder
         * @param kafkaProps    properties defining consumer connection to Kafka broker as specified in @see <a href="http://kafka.apache.org/090/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html">KafkaConsumer</a>
         * @param kafkaSpoutStreams    streams to where the tuples are emitted for each tuple. Multiple topics can emit in the same stream.
         */
        public Builder(Map<String, Object> kafkaProps, KafkaSpoutStreams kafkaSpoutStreams) {
            if (kafkaProps == null || kafkaProps.isEmpty()) {
                throw new IllegalArgumentException("Properties defining consumer connection to Kafka broker are required. " + kafkaProps);
            }

            if (kafkaSpoutStreams == null)  {
                throw new IllegalArgumentException("Must specify stream associated with each topic. Multiple topics can emit in the same stream.");
            }
            this.kafkaProps = kafkaProps;
            this.kafkaSpoutStreams = kafkaSpoutStreams;
        }

        /**
         * Specifying this key deserializer overrides the property key.deserializer
         */
        public Builder<K,V> setKeyDeserializer(Deserializer<K> keyDeserializer) {
            this.keyDeserializer = keyDeserializer;
            return this;
        }

        /**
         * Specifying this value deserializer overrides the property value.deserializer
         */
        public Builder<K,V> setValueDeserializer(Deserializer<V> valueDeserializer) {
            this.valueDeserializer = valueDeserializer;
            return this;
        }

        /**
         * Specifies the time, in milliseconds, spent waiting in poll if data is not available. Default is 2s
         * @param pollTimeoutMs time in ms
         */
        public Builder<K,V> setPollTimeoutMs(long pollTimeoutMs) {
            this.pollTimeoutMs = pollTimeoutMs;
            return this;
        }

        /**
         * Specifies the period, in milliseconds, the offset commit task is periodically called. Default is 15s.
         * @param offsetCommitPeriodMs time in ms
         */
        public Builder<K,V> setOffsetCommitPeriodMs(long offsetCommitPeriodMs) {
            this.offsetCommitPeriodMs = offsetCommitPeriodMs;
            return this;
        }

        /**
         * Defines the max number of retrials in case of tuple failure. The default is to retry forever, which means that
         * no new records are committed until the previous polled records have been acked. This guarantees at once delivery of
         * all the previously polled records.
         * By specifying a finite value for maxRetries, the user decides to sacrifice guarantee of delivery for the previous
         * polled records in favor of processing more records.
         * @param maxRetries max number of retrials
         */
        public Builder<K,V> setMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        /**
         * Defines the max number of polled offsets (records) that can be pending commit, before another poll can take place.
         * Once this limit is reached, no more offsets (records) can be polled until the next successful commit(s) sets the number
         * of pending offsets bellow the threshold. The default is {@link #DEFAULT_MAX_UNCOMMITTED_OFFSETS}.
         * @param maxUncommittedOffsets max number of records that can be be pending commit
         */
        public Builder<K,V> setMaxUncommittedOffsets(int maxUncommittedOffsets) {
            this.maxUncommittedOffsets = maxUncommittedOffsets;
            return this;
        }

        /**
         * Sets the offset used by the Kafka spout in the first poll to Kafka broker upon process start.
         * Please refer to to the documentation in {@link FirstPollOffsetStrategy}
         * @param firstPollOffsetStrategy Offset used by Kafka spout first poll
         * */
        public Builder<K, V> setFirstPollOffsetStrategy(FirstPollOffsetStrategy firstPollOffsetStrategy) {
            this.firstPollOffsetStrategy = firstPollOffsetStrategy;
            return this;
        }

        /**
         * Sets the strategy used by the the Kafka spout to decide when to poll the next batch of records from Kafka.
         * Please refer to to the documentation in {@link PollStrategy}
         * @param pollStrategy strategy used to decide when to poll
         * */
        public Builder<K, V> setPollStrategy(PollStrategy pollStrategy) {
            this.pollStrategy = pollStrategy;
            return this;
        }

        public KafkaSpoutConfig<K,V> build() {
            return new KafkaSpoutConfig<>(this);
        }
    }

    public Map<String, Object> getKafkaProps() {
        return kafkaProps;
    }

    public Deserializer<K> getKeyDeserializer() {
        return keyDeserializer;
    }

    public Deserializer<V> getValueDeserializer() {
        return valueDeserializer;
    }

    public long getPollTimeoutMs() {
        return pollTimeoutMs;
    }

    public long getOffsetsCommitPeriodMs() {
        return offsetCommitPeriodMs;
    }

    public boolean isConsumerAutoCommitMode() {
        return kafkaProps.get(Consumer.ENABLE_AUTO_COMMIT) == null     // default is true
                || Boolean.valueOf((String)kafkaProps.get(Consumer.ENABLE_AUTO_COMMIT));
    }

    public String getConsumerGroupId() {
        return (String) kafkaProps.get(Consumer.GROUP_ID);
    }

    public List<String> getSubscribedTopics() {
        return new ArrayList<>(kafkaSpoutStreams.getTopics());
    }

    public int getMaxTupleRetries() {
        return maxRetries;
    }

    public FirstPollOffsetStrategy getFirstPollOffsetStrategy() {
        return firstPollOffsetStrategy;
    }

    public KafkaSpoutStreams getKafkaSpoutStreams() {
        return kafkaSpoutStreams;
    }

    public int getMaxUncommittedOffsets() {
        return maxUncommittedOffsets;
    }

    public PollStrategy getPollStrategy() {
        return pollStrategy;
    }

    @Override
    public String toString() {
        return "KafkaSpoutConfig{" +
                "kafkaProps=" + kafkaProps +
                ", keyDeserializer=" + keyDeserializer +
                ", valueDeserializer=" + valueDeserializer +
                ", topics=" + getSubscribedTopics() +
                ", firstPollOffsetStrategy=" + firstPollOffsetStrategy +
                ", pollTimeoutMs=" + pollTimeoutMs +
                ", offsetCommitPeriodMs=" + offsetCommitPeriodMs +
                ", maxRetries=" + maxRetries +
                '}';
    }
}
