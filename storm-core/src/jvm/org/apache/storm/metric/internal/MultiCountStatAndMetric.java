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
 * Acts as a MultiCount Metric, but keeps track of approximate counts
 * for the last 10 mins, 3 hours, 1 day, and all time. for the same keys
 */
public class MultiCountStatAndMetric<T> implements IMetric {
    private ConcurrentHashMap<T, CountStatAndMetric> _counts = new ConcurrentHashMap<>();
    private final int _numBuckets;

    /**
     * @param numBuckets the number of buckets to divide the time periods into.
     */
    public MultiCountStatAndMetric(int numBuckets) {
        _numBuckets = numBuckets;
    }

    CountStatAndMetric get(T key) {
        CountStatAndMetric c = _counts.get(key);
        if (c == null) {
            synchronized(this) {
                c = _counts.get(key);
                if (c == null) {
                    c = new CountStatAndMetric(_numBuckets);
                    _counts.put(key, c);
                }
            }
        }
        return c;
    }

    /**
     * Increase the count by the given value.
     *
     * @param count number to count
     */
    public void incBy(T key, long count) {
        get(key).incBy(count);
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
        Map<String, Long> ret = new HashMap<String, Long>();
        for (Map.Entry<T, CountStatAndMetric> entry: _counts.entrySet()) {
            String key = keyToString(entry.getKey());
            //There could be collisions if keyToString returns only part of a result.
            Long val = (Long)entry.getValue().getValueAndReset();
            Long other = ret.get(key);
            val += other == null ? 0l : other;
            ret.put(key, val);
        }
        return ret;
    }

    public Map<String, Map<T, Long>> getTimeCounts() {
        Map<String, Map<T, Long>> ret = new HashMap<>();
        for (Map.Entry<T, CountStatAndMetric> entry: _counts.entrySet()) {
            T key = entry.getKey();
            Map<String, Long> toFlip = entry.getValue().getTimeCounts();
            for (Map.Entry<String, Long> subEntry: toFlip.entrySet()) {
                String time = subEntry.getKey();
                Map<T, Long> tmp = ret.get(time);
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
        for (CountStatAndMetric cc: _counts.values()) {
            cc.close();
        }
    }
}
