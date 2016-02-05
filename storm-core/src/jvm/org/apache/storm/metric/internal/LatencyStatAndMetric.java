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
import java.util.TimerTask;

import org.apache.storm.metric.api.IMetric;
import org.apache.storm.utils.Utils;

/**
 * Acts as a Latency Metric, but also keeps track of approximate latency
 * for the last 10 mins, 3 hours, 1 day, and all time.
 */
public class LatencyStatAndMetric implements IMetric {
    //The current lat and count buckets are protected by a different lock
    // from the other buckets.  This is to reduce the lock contention
    // When doing complex calculations.  Never grab the instance object lock
    // while holding _currentLock to avoid deadlocks
    private final Object _currentLock = new byte[0];
    private long _currentLatBucket;
    private long _currentCountBucket;

    // All internal state except for the current buckets are
    // protected using the Object Lock
    private long _bucketStart;

    //exact variable time, that is added to the current bucket
    private long _exactExtraLat;
    private long _exactExtraCount;
 
    //10 min values
    private final int _tmSize;
    private final long[] _tmLatBuckets;
    private final long[] _tmCountBuckets;
    private final long[] _tmTime;
    
    //3 hour values
    private final int _thSize;
    private final long[] _thLatBuckets;
    private final long[] _thCountBuckets;
    private final long[] _thTime;

    //1 day values
    private final int _odSize;
    private final long[] _odLatBuckets;
    private final long[] _odCountBuckets;
    private final long[] _odTime;
 
    //all time
    private long _allTimeLat;
    private long _allTimeCount;

    private final TimerTask _task;

    /**
     * @param numBuckets the number of buckets to divide the time periods into.
     */
    public LatencyStatAndMetric(int numBuckets) {
        this(numBuckets, -1);
    }

    /**
     * Constructor
     * @param numBuckets the number of buckets to divide the time periods into.
     * @param startTime if positive the simulated time to start the from.
     */
    LatencyStatAndMetric(int numBuckets, long startTime){
        numBuckets = Math.max(numBuckets, 2);
        //We want to capture the full time range, so the target size is as
        // if we had one bucket less, then we do
        _tmSize = 10 * 60 * 1000 / (numBuckets - 1);
        _thSize = 3 * 60 * 60 * 1000 / (numBuckets - 1);
        _odSize = 24 * 60 * 60 * 1000 / (numBuckets - 1);
        if (_tmSize < 1 || _thSize < 1 || _odSize < 1) {
            throw new IllegalArgumentException("number of buckets is too large to be supported");
        }
        _tmLatBuckets = new long[numBuckets];
        _tmCountBuckets = new long[numBuckets];
        _tmTime = new long[numBuckets];
        _thLatBuckets = new long[numBuckets];
        _thCountBuckets = new long[numBuckets];
        _thTime = new long[numBuckets];
        _odLatBuckets = new long[numBuckets];
        _odCountBuckets = new long[numBuckets];
        _odTime = new long[numBuckets];
        _allTimeLat = 0;
        _allTimeCount = 0;
        _exactExtraLat = 0;
        _exactExtraCount = 0;

        _bucketStart = startTime >= 0 ? startTime : System.currentTimeMillis();
        _currentLatBucket = 0;
        _currentCountBucket = 0;
        if (startTime < 0) {
            _task = new Fresher();
            MetricStatTimer._timer.scheduleAtFixedRate(_task, _tmSize, _tmSize);
        } else {
            _task = null;
        }
    }

    /**
     * Record a specific latency
     *
     * @param latency what we are recording
     */
    public void record(long latency) {
        synchronized(_currentLock) {
            _currentLatBucket += latency;
            _currentCountBucket++;
        }
    }

    @Override
    public synchronized Object getValueAndReset() {
        return getValueAndReset(System.currentTimeMillis());
    }

    synchronized Object getValueAndReset(long now) {
        long lat;
        long count;
        synchronized(_currentLock) {
            lat = _currentLatBucket;
            count = _currentCountBucket;
            _currentLatBucket = 0;
            _currentCountBucket = 0;
        }

        long timeSpent = now - _bucketStart;
        long exactExtraCountSum = count + _exactExtraCount;
        double ret = Utils.zeroIfNaNOrInf(
                ((double) (lat + _exactExtraLat)) / exactExtraCountSum);
        _bucketStart = now;
        _exactExtraLat = 0;
        _exactExtraCount = 0;
        rotateBuckets(lat, count, timeSpent);
        return ret;
    }

    synchronized void rotateSched(long now) {
        long lat;
        long count;
        synchronized(_currentLock) {
            lat = _currentLatBucket;
            count = _currentCountBucket;
            _currentLatBucket = 0;
            _currentCountBucket = 0;
        }

        long timeSpent = now - _bucketStart;
        _exactExtraLat += lat;
        _exactExtraCount += count;
        _bucketStart = now;
        rotateBuckets(lat, count, timeSpent);
    }

    synchronized void rotateBuckets(long lat, long count, long timeSpent) {
        rotate(lat, count, timeSpent, _tmSize, _tmTime, _tmLatBuckets, _tmCountBuckets);
        rotate(lat, count, timeSpent, _thSize, _thTime, _thLatBuckets, _thCountBuckets);
        rotate(lat, count, timeSpent, _odSize, _odTime, _odLatBuckets, _odCountBuckets);
        _allTimeLat += lat;
        _allTimeCount += count;
    }

    private synchronized void rotate(long lat, long count, long timeSpent, long targetSize,
            long [] times, long [] latBuckets, long [] countBuckets) {
        times[0] += timeSpent;
        latBuckets[0] += lat;
        countBuckets[0] += count;

        long currentTime = 0;
        long currentLat = 0;
        long currentCount = 0;
        if (times[0] >= targetSize) {
            for (int i = 0; i < latBuckets.length; i++) {
                long tmpTime = times[i];
                times[i] = currentTime;
                currentTime = tmpTime;

                long lt = latBuckets[i];
                latBuckets[i] = currentLat;
                currentLat = lt;

                long cnt = countBuckets[i];
                countBuckets[i] = currentCount;
                currentCount = cnt;
            }
        }
    }

    /**
     * @return a map of time window to average latency.
     * Keys are "600" for last 10 mins
     * "10800" for the last 3 hours
     * "86400" for the last day
     * ":all-time" for all time
     */
    public synchronized Map<String, Double> getTimeLatAvg() {
        return getTimeLatAvg(System.currentTimeMillis());
    }

    synchronized Map<String, Double> getTimeLatAvg(long now) {
        Map<String, Double> ret = new HashMap<>();
        long lat;
        long count;
        synchronized(_currentLock) {
            lat = _currentLatBucket;
            count = _currentCountBucket;
        }
        long timeSpent = now - _bucketStart;
        ret.put("600", readApproximateLatAvg(lat, count, timeSpent, _tmTime, _tmLatBuckets, _tmCountBuckets, 600 * 1000));
        ret.put("10800", readApproximateLatAvg(lat, count, timeSpent, _thTime, _thLatBuckets, _thCountBuckets, 10800 * 1000));
        ret.put("86400", readApproximateLatAvg(lat, count, timeSpent, _odTime, _odLatBuckets, _odCountBuckets, 86400 * 1000));
        long allTimeCountSum = count + _allTimeCount;
        ret.put(":all-time", Utils.zeroIfNaNOrInf(
                (double) lat + _allTimeLat)/allTimeCountSum);
        return ret;
    }

    double readApproximateLatAvg(long lat, long count, long timeSpent, long[] bucketTime,
              long[] latBuckets, long[] countBuckets, long desiredTime) {
        long timeNeeded = desiredTime - timeSpent;
        long totalLat = lat;
        long totalCount = count;
        for (int i = 0; i < bucketTime.length && timeNeeded > 0; i++) {
            //Don't pro-rate anything, it is all approximate so an extra bucket is not that bad.
            totalLat += latBuckets[i];
            totalCount += countBuckets[i];
            timeNeeded -= bucketTime[i];
        }
        return Utils.zeroIfNaNOrInf(((double) totalLat) / totalCount);
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
