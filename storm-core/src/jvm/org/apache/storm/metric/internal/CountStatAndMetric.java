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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.metric.internal;

import java.util.Map;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.storm.metric.api.IMetric;

/**
 * Acts as a Count Metric, but also keeps track of approximate counts
 * for the last 10 mins, 3 hours, 1 day, and all time.
 */
public class CountStatAndMetric implements IMetric{
    private final AtomicLong _currentBucket;
    // All internal state except for the count of the current bucket are
    // protected using a lock on this counter
    private long _bucketStart;

    //exact variable time, that is added to the current bucket
    private long _exactExtra;
 
    //10 min values
    private final int _tmSize;
    private final long[] _tmBuckets;
    private final long[] _tmTime;
    
    //3 hour values
    private final int _thSize;
    private final long[] _thBuckets;
    private final long[] _thTime;

    //1 day values
    private final int _odSize;
    private final long[] _odBuckets;
    private final long[] _odTime;
 
    //all time
    private long _allTime;

    private final TimerTask _task;

    /**
     * @param numBuckets the number of buckets to divide the time periods into.
     */
    public CountStatAndMetric(int numBuckets) {
        this(numBuckets, -1);
    }

    /**
     * Constructor
     * @param numBuckets the number of buckets to divide the time periods into.
     * @param startTime if positive the simulated time to start the from.
     */
    CountStatAndMetric(int numBuckets, long startTime){
        numBuckets = Math.max(numBuckets, 2);
        //We want to capture the full time range, so the target size is as
        // if we had one bucket less, then we do
        _tmSize = 10 * 60 * 1000 / (numBuckets - 1);
        _thSize = 3 * 60 * 60 * 1000 / (numBuckets - 1);
        _odSize = 24 * 60 * 60 * 1000 / (numBuckets - 1);
        if (_tmSize < 1 || _thSize < 1 || _odSize < 1) {
            throw new IllegalArgumentException("number of buckets is too large to be supported");
        }
        _tmBuckets = new long[numBuckets];
        _tmTime = new long[numBuckets];
        _thBuckets = new long[numBuckets];
        _thTime = new long[numBuckets];
        _odBuckets = new long[numBuckets];
        _odTime = new long[numBuckets];
        _allTime = 0;
        _exactExtra = 0;

        _bucketStart = startTime >= 0 ? startTime : System.currentTimeMillis();
        _currentBucket = new AtomicLong(0);
        if (startTime < 0) {
            _task = new Fresher();
            MetricStatTimer._timer.scheduleAtFixedRate(_task, _tmSize, _tmSize);
        } else {
            _task = null;
        }
    }

    /**
     * Increase the count by the given value.
     *
     * @param count number to count
     */
    public void incBy(long count) {
        _currentBucket.addAndGet(count);
    }

   

    @Override
    public synchronized Object getValueAndReset() {
        return getValueAndReset(System.currentTimeMillis());
    }

    synchronized Object getValueAndReset(long now) {
        long value = _currentBucket.getAndSet(0);
        long timeSpent = now - _bucketStart;
        long ret = value + _exactExtra;
        _bucketStart = now;
        _exactExtra = 0;
        rotateBuckets(value, timeSpent);
        return ret;
    }

    synchronized void rotateSched(long now) {
        long value = _currentBucket.getAndSet(0);
        long timeSpent = now - _bucketStart;
        _exactExtra += value;
        _bucketStart = now;
        rotateBuckets(value, timeSpent);
    }

    synchronized void rotateBuckets(long value, long timeSpent) {
        rotate(value, timeSpent, _tmSize, _tmTime, _tmBuckets);
        rotate(value, timeSpent, _thSize, _thTime, _thBuckets);
        rotate(value, timeSpent, _odSize, _odTime, _odBuckets);
        _allTime += value;
    }

    private synchronized void rotate(long value, long timeSpent, long targetSize, long [] times, long [] buckets) {
        times[0] += timeSpent;
        buckets[0] += value;

        long currentTime = 0;
        long currentVal = 0;
        if (times[0] >= targetSize) {
            for (int i = 0; i < buckets.length; i++) {
                long tmpTime = times[i];
                times[i] = currentTime;
                currentTime = tmpTime;

                long cnt = buckets[i];
                buckets[i] = currentVal;
                currentVal = cnt;
            }
        }
    }

    /**
     * @return a map of time window to count.
     * Keys are "600" for last 10 mins
     * "10800" for the last 3 hours
     * "86400" for the last day
     * ":all-time" for all time
     */
    public synchronized Map<String, Long> getTimeCounts() {
        return getTimeCounts(System.currentTimeMillis());
    }

    synchronized Map<String, Long> getTimeCounts(long now) {
        Map<String, Long> ret = new HashMap<>();
        long value = _currentBucket.get();
        long timeSpent = now - _bucketStart;
        ret.put("600", readApproximateTime(value, timeSpent, _tmTime, _tmBuckets, 600 * 1000));
        ret.put("10800", readApproximateTime(value, timeSpent, _thTime, _thBuckets, 10800 * 1000));
        ret.put("86400", readApproximateTime(value, timeSpent, _odTime, _odBuckets, 86400 * 1000));
        ret.put(":all-time", value + _allTime);
        return ret;
    }

    long readApproximateTime(long value, long timeSpent, long[] bucketTime, long[] buckets, long desiredTime) {
        long timeNeeded = desiredTime - timeSpent;
        long total = value;
        for (int i = 0; i < bucketTime.length; i++) {
            if (timeNeeded < bucketTime[i]) {
                double pct = timeNeeded/((double)bucketTime[i]);
                total += (long)(pct * buckets[i]);
                timeNeeded = 0;
                break;
            }
            total += buckets[i];
            timeNeeded -= bucketTime[i];
        }
        return total;
    }

    public void close() {
        if (_task != null) {
            _task.cancel();
        }
    }

    private class Fresher extends TimerTask {
        public void run () {
            rotateSched(System.currentTimeMillis());
        }
    }
}
