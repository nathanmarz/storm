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
package org.apache.storm.stats;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.metric.internal.MultiCountStatAndMetric;
import org.apache.storm.metric.internal.MultiLatencyStatAndMetric;

@SuppressWarnings("unchecked")
public class CommonStats {
    public static final int NUM_STAT_BUCKETS = 20;

    public static final String RATE = "rate";

    public static final String EMITTED = "emitted";
    public static final String TRANSFERRED = "transferred";
    public static final String[] COMMON_FIELDS = {EMITTED, TRANSFERRED};

    protected final int rate;
    protected final Map<String, IMetric> metricMap = new HashMap<>();

    public CommonStats(int rate) {
        this.rate = rate;
        this.put(EMITTED, new MultiCountStatAndMetric(NUM_STAT_BUCKETS));
        this.put(TRANSFERRED, new MultiCountStatAndMetric(NUM_STAT_BUCKETS));
    }

    public int getRate() {
        return this.rate;
    }

    public MultiCountStatAndMetric getEmitted() {
        return (MultiCountStatAndMetric) get(EMITTED);
    }

    public MultiCountStatAndMetric getTransferred() {
        return (MultiCountStatAndMetric) get(TRANSFERRED);
    }

    public IMetric get(String field) {
        return (IMetric) StatsUtil.getByKey(metricMap, field);
    }

    protected void put(String field, Object value) {
        StatsUtil.putKV(metricMap, field, value);
    }

    public void emittedTuple(String stream) {
        this.getEmitted().incBy(stream, this.rate);
    }

    public void transferredTuples(String stream, int amount) {
        this.getTransferred().incBy(stream, this.rate * amount);
    }

    public void cleanupStats() {
        for (Object imetric : this.metricMap.values()) {
            cleanupStat((IMetric) imetric);
        }
    }

    private void cleanupStat(IMetric metric) {
        if (metric instanceof MultiCountStatAndMetric) {
            ((MultiCountStatAndMetric) metric).close();
        } else if (metric instanceof MultiLatencyStatAndMetric) {
            ((MultiLatencyStatAndMetric) metric).close();
        }
    }

    protected Map valueStats(String[] fields) {
        Map ret = new HashMap();
        for (String field : fields) {
            IMetric metric = this.get(field);
            if (metric instanceof MultiCountStatAndMetric) {
                StatsUtil.putKV(ret, field, ((MultiCountStatAndMetric) metric).getTimeCounts());
            } else if (metric instanceof MultiLatencyStatAndMetric) {
                StatsUtil.putKV(ret, field, ((MultiLatencyStatAndMetric) metric).getTimeLatAvg());
            }
        }
        StatsUtil.putKV(ret, CommonStats.RATE, this.getRate());

        return ret;
    }

    protected Map valueStat(String field) {
        IMetric metric = this.get(field);
        if (metric instanceof MultiCountStatAndMetric) {
            return ((MultiCountStatAndMetric) metric).getTimeCounts();
        } else if (metric instanceof MultiLatencyStatAndMetric) {
            return ((MultiLatencyStatAndMetric) metric).getTimeLatAvg();
        }
        return null;
    }

}
