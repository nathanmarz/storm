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

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of {@link KafkaSpoutRetryService} using the exponential backoff formula. The time of the nextRetry is set as follows:
 * nextRetry = failCount == 1 ? currentTime + initialDelay : currentTime + delayPeriod^(failCount-1)    where failCount = 1, 2, 3, ...
 * nextRetry = Min(nextRetry, currentTime + maxDelay)
 */
public class KafkaSpoutRetryExponentialBackoff implements KafkaSpoutRetryService {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSpoutRetryExponentialBackoff.class);
    private static final RetryEntryTimeStampComparator RETRY_ENTRY_TIME_STAMP_COMPARATOR = new RetryEntryTimeStampComparator();

    private TimeInterval initialDelay;
    private TimeInterval delayPeriod;
    private TimeInterval maxDelay;
    private int maxRetries;

    private Set<RetrySchedule> retrySchedules = new TreeSet<>(RETRY_ENTRY_TIME_STAMP_COMPARATOR);
    private Set<KafkaSpoutMessageId> toRetryMsgs = new HashSet<>();      // Convenience data structure to speedup lookups

    /**
     * Comparator ordering by timestamp 
     */
    private static class RetryEntryTimeStampComparator implements Serializable, Comparator<RetrySchedule> {
        public int compare(RetrySchedule entry1, RetrySchedule entry2) {
            return Long.valueOf(entry1.nextRetryTimeNanos()).compareTo(entry2.nextRetryTimeNanos());
        }
    }

    private class RetrySchedule {
        private KafkaSpoutMessageId msgId;
        private long nextRetryTimeNanos;

        public RetrySchedule(KafkaSpoutMessageId msgId, long nextRetryTime) {
            this.msgId = msgId;
            this.nextRetryTimeNanos = nextRetryTime;
            LOG.debug("Created {}", this);
        }

        public void setNextRetryTime() {
            nextRetryTimeNanos = nextTime(msgId);
            LOG.debug("Updated {}", this);
        }

        public boolean retry(long currentTimeNanos) {
            return nextRetryTimeNanos <= currentTimeNanos;
        }

        @Override
        public String toString() {
            return "RetrySchedule{" +
                    "msgId=" + msgId +
                    ", nextRetryTime=" + nextRetryTimeNanos +
                    '}';
        }

        public KafkaSpoutMessageId msgId() {
            return msgId;
        }

        public long nextRetryTimeNanos() {
            return nextRetryTimeNanos;
        }
    }

    public static class TimeInterval implements Serializable {
        private long lengthNanos;
        private long length;
        private TimeUnit timeUnit;

        /**
         * @param length length of the time interval in the units specified by {@link TimeUnit}
         * @param timeUnit unit used to specify a time interval on which to specify a time unit
         */
        public TimeInterval(long length, TimeUnit timeUnit) {
            this.length = length;
            this.timeUnit = timeUnit;
            this.lengthNanos = timeUnit.toNanos(length);
        }

        public static TimeInterval seconds(long length) {
            return new TimeInterval(length, TimeUnit.SECONDS);
        }

        public static TimeInterval milliSeconds(long length) {
            return new TimeInterval(length, TimeUnit.MILLISECONDS);
        }

        public static TimeInterval microSeconds(long length) {
            return new TimeInterval(length, TimeUnit.MILLISECONDS);
        }

        public long lengthNanos() {
            return lengthNanos;
        }

        public long length() {
            return length;
        }

        public TimeUnit timeUnit() {
            return timeUnit;
        }

        @Override
        public String toString() {
            return "TimeInterval{" +
                    "length=" + length +
                    ", timeUnit=" + timeUnit +
                    '}';
        }
    }

    /**
     * The time stamp of the next retry is scheduled according to the exponential backoff formula ( geometric progression):
     * nextRetry = failCount == 1 ? currentTime + initialDelay : currentTime + delayPeriod^(failCount-1) where failCount = 1, 2, 3, ...
     * nextRetry = Min(nextRetry, currentTime + maxDelay)
     *
     * @param initialDelay      initial delay of the first retry
     * @param delayPeriod       the time interval that is the ratio of the exponential backoff formula (geometric progression)
     * @param maxRetries        maximum number of times a tuple is retried before being acked and scheduled for commit
     * @param maxDelay          maximum amount of time waiting before retrying
     *
     */
    public KafkaSpoutRetryExponentialBackoff(TimeInterval initialDelay, TimeInterval delayPeriod, int maxRetries, TimeInterval maxDelay) {
        this.initialDelay = initialDelay;
        this.delayPeriod = delayPeriod;
        this.maxRetries = maxRetries;
        this.maxDelay = maxDelay;
        LOG.debug("Instantiated {}", this);
    }

    @Override
    public Set<TopicPartition> retriableTopicPartitions() {
        final Set<TopicPartition> tps = new TreeSet<>();
        final long currentTimeNanos = System.nanoTime();
        for (RetrySchedule retrySchedule : retrySchedules) {
            if (retrySchedule.retry(currentTimeNanos)) {
                final KafkaSpoutMessageId msgId = retrySchedule.msgId;
                tps.add(new TopicPartition(msgId.topic(), msgId.partition()));
            } else {
                break;  // Stop searching as soon as passed current time
            }
        }
        LOG.debug("Topic partitions with entries ready to be retried [{}] ", tps);
        return tps;
    }

    @Override
    public boolean isReady(KafkaSpoutMessageId msgId) {
        boolean retry = false;
        if (toRetryMsgs.contains(msgId)) {
            final long currentTimeNanos = System.nanoTime();
            for (RetrySchedule retrySchedule : retrySchedules) {
                if (retrySchedule.retry(currentTimeNanos)) {
                    if (retrySchedule.msgId.equals(msgId)) {
                        retry = true;
                        LOG.debug("Found entry to retry {}", retrySchedule);
                    }
                } else {
                    LOG.debug("Entry to retry not found {}", retrySchedule);
                    break;  // Stop searching as soon as passed current time
                }
            }
        }
        return retry;
    }

    @Override
    public boolean isScheduled(KafkaSpoutMessageId msgId) {
        return toRetryMsgs.contains(msgId);
    }

    @Override
    public boolean remove(KafkaSpoutMessageId msgId) {
        boolean removed = false;
        if (toRetryMsgs.contains(msgId)) {
            for (Iterator<RetrySchedule> iterator = retrySchedules.iterator(); iterator.hasNext(); ) {
                final RetrySchedule retrySchedule = iterator.next();
                if (retrySchedule.msgId().equals(msgId)) {
                    iterator.remove();
                    toRetryMsgs.remove(msgId);
                    removed = true;
                    break;
                }
            }
        }
        LOG.debug(removed ? "Removed {} " : "Not removed {}", msgId);
        LOG.trace("Current state {}", retrySchedules);
        return removed;
    }

    @Override
    public boolean retainAll(Collection<TopicPartition> topicPartitions) {
        boolean result = false;
        for (Iterator<RetrySchedule> rsIterator = retrySchedules.iterator(); rsIterator.hasNext(); ) {
            final RetrySchedule retrySchedule = rsIterator.next();
            final KafkaSpoutMessageId msgId = retrySchedule.msgId;
            final TopicPartition tpRetry= new TopicPartition(msgId.topic(), msgId.partition());
            if (!topicPartitions.contains(tpRetry)) {
                rsIterator.remove();
                toRetryMsgs.remove(msgId);
                LOG.debug("Removed {}", retrySchedule);
                LOG.trace("Current state {}", retrySchedules);
                result = true;
            }
        }
        return result;
    }

    @Override
    public void schedule(KafkaSpoutMessageId msgId) {
        if (msgId.numFails() > maxRetries) {
            LOG.debug("Not scheduling [{}] because reached maximum number of retries [{}].", msgId, maxRetries);
        } else {
            if (toRetryMsgs.contains(msgId)) {
                for (Iterator<RetrySchedule> iterator = retrySchedules.iterator(); iterator.hasNext(); ) {
                    final RetrySchedule retrySchedule = iterator.next();
                    if (retrySchedule.msgId().equals(msgId)) {
                        iterator.remove();
                        toRetryMsgs.remove(msgId);
                    }
                }
            }
            final RetrySchedule retrySchedule = new RetrySchedule(msgId, nextTime(msgId));
            retrySchedules.add(retrySchedule);
            toRetryMsgs.add(msgId);
            LOG.debug("Scheduled. {}", retrySchedule);
            LOG.trace("Current state {}", retrySchedules);
        }
    }

    // if value is greater than Long.MAX_VALUE it truncates to Long.MAX_VALUE
    private long nextTime(KafkaSpoutMessageId msgId) {
        final long currentTimeNanos = System.nanoTime();
        final long nextTimeNanos = msgId.numFails() == 1                // numFails = 1, 2, 3, ...
                ? currentTimeNanos + initialDelay.lengthNanos()
                : (long) (currentTimeNanos + Math.pow(delayPeriod.lengthNanos, msgId.numFails() - 1));
        return Math.min(nextTimeNanos, currentTimeNanos + maxDelay.lengthNanos);
    }

    @Override
    public String toString() {
        return "KafkaSpoutRetryExponentialBackoff{" +
                "delay=" + initialDelay +
                ", ratio=" + delayPeriod +
                ", maxRetries=" + maxRetries +
                ", maxRetryDelay=" + maxDelay +
                '}';
    }
}
