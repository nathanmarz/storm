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

import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class is a utility to track the rate of something.
 */
public class RateTracker{
    private final int _bucketSizeMillis;
    //Old Buckets and their length are only touched when rotating or gathering the metrics, which should not be that frequent
    // As such all access to them should be protected by synchronizing with the RateTracker instance
    private final long[] _bucketTime;
    private final long[] _oldBuckets;
    
    private final AtomicLong _bucketStart;
    private final AtomicLong _currentBucket;
    
    private final TimerTask _task;

    /**
     * @param validTimeWindowInMils events that happened before validTimeWindowInMils are not considered
     *                        when reporting the rate.
     * @param numBuckets the number of time sildes to divide validTimeWindows. The more buckets,
     *                    the smother the reported results will be.
     */
    public RateTracker(int validTimeWindowInMils, int numBuckets) {
        this(validTimeWindowInMils, numBuckets, -1);
    }

    /**
     * Constructor
     * @param validTimeWindowInMils events that happened before validTimeWindow are not considered
     *                        when reporting the rate.
     * @param numBuckets the number of time sildes to divide validTimeWindows. The more buckets,
     *                    the smother the reported results will be.
     * @param startTime if positive the simulated time to start the first bucket at.
     */
    RateTracker(int validTimeWindowInMils, int numBuckets, long startTime){
        numBuckets = Math.max(numBuckets, 1);
        _bucketSizeMillis = validTimeWindowInMils / numBuckets;
        if (_bucketSizeMillis < 1 ) {
            throw new IllegalArgumentException("validTimeWindowInMilis and numOfSildes cause each slide to have a window that is too small");
        }
        _bucketTime = new long[numBuckets - 1];
        _oldBuckets = new long[numBuckets - 1];

        _bucketStart = new AtomicLong(startTime >= 0 ? startTime : System.currentTimeMillis());
        _currentBucket = new AtomicLong(0);
        if (startTime < 0) {
            _task = new Fresher();
            MetricStatTimer._timer.scheduleAtFixedRate(_task, _bucketSizeMillis, _bucketSizeMillis);
        } else {
            _task = null;
        }
    }

    /**
     * Notify the tracker upon new arrivals
     *
     * @param count number of arrivals
     */
    public void notify(long count) {
        _currentBucket.addAndGet(count);
    }

    /**
     * @return the approximate average rate per second.
     */
    public synchronized double reportRate() {
        return reportRate(System.currentTimeMillis());
    }

    synchronized double reportRate(long currentTime) {
        long duration = Math.max(1l, currentTime - _bucketStart.get());
        long events = _currentBucket.get();
        for (int i = 0; i < _oldBuckets.length; i++) {
            events += _oldBuckets[i];
            duration += _bucketTime[i];
        }

        return events * 1000.0 / duration;
    }

    public void close() {
        if (_task != null) {
            _task.cancel();
        }
    }

    /**
     * Rotate the buckets a set number of times for testing purposes.
     * @param numToEclipse the number of rotations to perform.
     */
    final void forceRotate(int numToEclipse, long interval) {
        long time = _bucketStart.get();
        for (int i = 0; i < numToEclipse; i++) {
            time += interval;
            rotateBuckets(time);
        }
    }

    private synchronized void rotateBuckets(long time) {
        long timeSpent = time - _bucketStart.getAndSet(time); 
        long currentVal = _currentBucket.getAndSet(0);
        for (int i = 0; i < _oldBuckets.length; i++) {
            long tmpTime = _bucketTime[i];
            _bucketTime[i] = timeSpent;
            timeSpent = tmpTime;

            long cnt = _oldBuckets[i];
            _oldBuckets[i] = currentVal;
            currentVal = cnt;
        }
    }

    private class Fresher extends TimerTask {
        public void run () {
            rotateBuckets(System.currentTimeMillis());
        }
    }
}
