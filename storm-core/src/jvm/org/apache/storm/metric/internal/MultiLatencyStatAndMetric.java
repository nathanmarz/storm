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
import java.util.List;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.metric.api.IMetric;

/**
 * Acts as a Latnecy Metric for multiple keys, but keeps track of approximate counts
 * for the last 10 mins, 3 hours, 1 day, and all time. for the same keys
 */
public class MultiLatencyStatAndMetric<T> implements IMetric {
    private ConcurrentHashMap<T, LatencyStatAndMetric> _lat = new ConcurrentHashMap<>();
    private final int _numBuckets;

    /**
     * @param numBuckets the number of buckets to divide the time periods into.
     */
    public MultiLatencyStatAndMetric(int numBuckets) {
        _numBuckets = numBuckets;
    }

    LatencyStatAndMetric get(T key) {
        LatencyStatAndMetric c = _lat.get(key);
        if (c == null) {
            synchronized(this) {
                c = _lat.get(key);
                if (c == null) {
                    c = new LatencyStatAndMetric(_numBuckets);
                    _lat.put(key, c);
                }
            }
        }
        return c;
    }

    /**
     * Record a latency value
     *
     * @param latency the measurement to record
     */
    public void record(T key, long latency) {
        get(key).record(latency);
    }

    protected String keyToString(T key) {
        if (key instanceof List) {
            //This is a bit of a hack.  If it is a list, then it is [component, stream]
            //we want to format this as component:stream
            List<String> lk = (List<String>)key;
            return lk.get(0) + ":" + lk.get(1);
        }
        return key.toString();
    }

    @Override
    public Object getValueAndReset() {
        Map<String, Double> ret = new HashMap<String, Double>();
        for (Map.Entry<T, LatencyStatAndMetric> entry: _lat.entrySet()) {
            String key = keyToString(entry.getKey());
            Double val = (Double)entry.getValue().getValueAndReset();
            ret.put(key, val);
        }
        return ret;
    }

    public Map<String, Map<T, Double>> getTimeLatAvg() {
        Map<String, Map<T, Double>> ret = new HashMap<>();
        for (Map.Entry<T, LatencyStatAndMetric> entry: _lat.entrySet()) {
            T key = entry.getKey();
            Map<String, Double> toFlip = entry.getValue().getTimeLatAvg();
            for (Map.Entry<String, Double> subEntry: toFlip.entrySet()) {
                String time = subEntry.getKey();
                Map<T, Double> tmp = ret.get(time);
                if (tmp == null) {
                    tmp = new HashMap<>();
                    ret.put(time, tmp);
                }
                tmp.put(key, subEntry.getValue());
            }
        }
        return ret;
    }

    public void close() {
        for (LatencyStatAndMetric l: _lat.values()) {
            l.close();
        }
    }
}
