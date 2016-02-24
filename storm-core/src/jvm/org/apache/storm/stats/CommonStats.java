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

public class CommonStats {
    public static final int NUM_STAT_BUCKETS = 20;

    public static final String RATE = "rate";

    public static final String EMITTED = "emitted";
    public static final String TRANSFERRED = "transferred";
    public static final String[] COMMON_FIELDS = {EMITTED, TRANSFERRED};

    protected int rate;
    protected final Map metricMap = new HashMap();

    public CommonStats() {
        put(EMITTED, new MultiCountStatAndMetric(NUM_STAT_BUCKETS));
        put(TRANSFERRED, new MultiCountStatAndMetric(NUM_STAT_BUCKETS));
    }

    public int getRate() {
        return this.rate;
    }

    public void setRate(int rate) {
        this.rate = rate;
    }

    public MultiCountStatAndMetric getEmitted() {
        return (MultiCountStatAndMetric) get(EMITTED);
    }

    public MultiCountStatAndMetric getTransferred() {
        return (MultiCountStatAndMetric) get(TRANSFERRED);
    }

    public IMetric get(String field) {
        return (IMetric) StatsUtil.getByKeyword(metricMap, field);
    }

    protected void put(String field, Object value) {
        StatsUtil.putRawKV(metricMap, field, value);
    }
}
