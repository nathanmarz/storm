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
package storm.kafka;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ExponentialBackoffMsgRetryManager implements FailedMsgRetryManager {

    private final long retryInitialDelayMs;
    private final double retryDelayMultiplier;
    private final long retryDelayMaxMs;

    private Queue<MessageRetryRecord> waiting = new PriorityQueue<MessageRetryRecord>(11, new RetryTimeComparator());
    private Map<Long,MessageRetryRecord> records = new ConcurrentHashMap<Long,MessageRetryRecord>();

    public ExponentialBackoffMsgRetryManager(long retryInitialDelayMs, double retryDelayMultiplier, long retryDelayMaxMs) {
        this.retryInitialDelayMs = retryInitialDelayMs;
        this.retryDelayMultiplier = retryDelayMultiplier;
        this.retryDelayMaxMs = retryDelayMaxMs;
    }

    @Override
    public void failed(Long offset) {
        MessageRetryRecord oldRecord = this.records.get(offset);
        MessageRetryRecord newRecord = oldRecord == null ?
                                       new MessageRetryRecord(offset) :
                                       oldRecord.createNextRetryRecord();
        this.records.put(offset, newRecord);
        this.waiting.add(newRecord);
    }

    @Override
    public void acked(Long offset) {
        MessageRetryRecord record = this.records.remove(offset);
        if (record != null) {
            this.waiting.remove(record);
        }
    }

    @Override
    public void retryStarted(Long offset) {
        MessageRetryRecord record = this.records.get(offset);
        if (record == null || !this.waiting.contains(record)) {
            throw new IllegalStateException("cannot retry a message that has not failed");
        } else {
            this.waiting.remove(record);
        }
    }

    @Override
    public Long nextFailedMessageToRetry() {
        if (this.waiting.size() > 0) {
            MessageRetryRecord first = this.waiting.peek();
            if (System.currentTimeMillis() >= first.retryTimeUTC) {
                if (this.records.containsKey(first.offset)) {
                    return first.offset;
                } else {
                    // defensive programming - should be impossible
                    this.waiting.remove(first);
                    return nextFailedMessageToRetry();
                }
            }
        }
        return null;
    }

    @Override
    public boolean shouldRetryMsg(Long offset) {
        MessageRetryRecord record = this.records.get(offset);
        return record != null &&
                this.waiting.contains(record) &&
                System.currentTimeMillis() >= record.retryTimeUTC;
    }

    @Override
    public Set<Long> clearInvalidMessages(Long kafkaOffset) {
        Set<Long> invalidOffsets = new HashSet<Long>(); 
        for(Long offset : records.keySet()){
            if(offset < kafkaOffset){
                MessageRetryRecord record = this.records.remove(offset);
                if (record != null) {
                    this.waiting.remove(record);
                    invalidOffsets.add(offset);
                }
            }
        }
        return invalidOffsets;
    }

    /**
     * A MessageRetryRecord holds the data of how many times a message has
     * failed and been retried, and when the last failure occurred.  It can
     * determine whether it is ready to be retried by employing an exponential
     * back-off calculation using config values stored in SpoutConfig:
     * <ul>
     *  <li>retryInitialDelayMs - time to delay before the first retry</li>
     *  <li>retryDelayMultiplier - multiplier by which to increase the delay for each subsequent retry</li>
     *  <li>retryDelayMaxMs - maximum retry delay (once this delay time is reached, subsequent retries will
     *                        delay for this amount of time every time)
     *  </li>
     * </ul>
     */
    private class MessageRetryRecord {
        private final long offset;
        private final int retryNum;
        private final long retryTimeUTC;

        public MessageRetryRecord(long offset) {
            this(offset, 1);
        }

        private MessageRetryRecord(long offset, int retryNum) {
            this.offset = offset;
            this.retryNum = retryNum;
            this.retryTimeUTC = System.currentTimeMillis() + calculateRetryDelay();
        }

        /**
         * Create a MessageRetryRecord for the next retry that should occur after this one.
         * @return MessageRetryRecord with the next retry time, or null to indicate that another
         *         retry should not be performed.  The latter case can happen if we are about to
         *         run into the backtype.storm.Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS in the Storm
         *         configuration.
         */
        public MessageRetryRecord createNextRetryRecord() {
            return new MessageRetryRecord(this.offset, this.retryNum + 1);
        }

        private long calculateRetryDelay() {
            double delayMultiplier = Math.pow(retryDelayMultiplier, this.retryNum - 1);
            double delay = retryInitialDelayMs * delayMultiplier;
            Long maxLong = Long.MAX_VALUE;
            long delayThisRetryMs = delay >= maxLong.doubleValue()
                                    ?  maxLong
                                    : (long) delay;
            return Math.min(delayThisRetryMs, retryDelayMaxMs);
        }

        @Override
        public boolean equals(Object other) {
            return (other instanceof MessageRetryRecord
                    && this.offset == ((MessageRetryRecord) other).offset);
        }

        @Override
        public int hashCode() {
            return Long.valueOf(this.offset).hashCode();
        }
    }

    private static class RetryTimeComparator implements Comparator<MessageRetryRecord> {

        @Override
        public int compare(MessageRetryRecord record1, MessageRetryRecord record2) {
            return Long.valueOf(record1.retryTimeUTC).compareTo(Long.valueOf(record2.retryTimeUTC));
        }

        @Override
        public boolean equals(Object obj) {
            return false;
        }
    }
}
