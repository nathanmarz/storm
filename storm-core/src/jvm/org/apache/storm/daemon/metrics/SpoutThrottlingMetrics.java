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
package org.apache.storm.daemon.metrics;

import org.apache.storm.metric.api.CountMetric;
import org.apache.storm.stats.CommonStats;

public class SpoutThrottlingMetrics extends BuiltinMetrics {
    private final CountMetric skippedMaxSpout = new CountMetric();
    private final CountMetric skippedThrottle = new CountMetric();
    private final CountMetric skippedInactive = new CountMetric();

    public SpoutThrottlingMetrics() {
        this.metricMap.put("skipped-max-spout", skippedMaxSpout);
        this.metricMap.put("skipped-throttle", skippedThrottle);
        this.metricMap.put("skipped-inactive", skippedInactive);
    }

    public CountMetric getSkippedMaxSpout() {
        return skippedMaxSpout;
    }

    public CountMetric getSkippedThrottle() {
        return skippedThrottle;
    }

    public CountMetric getSkippedInactive() {
        return skippedInactive;
    }

    public void skippedMaxSpout(CommonStats stats) {
        this.skippedMaxSpout.incrBy(stats.getRate());
    }

    public void skippedThrottle(CommonStats stats) {
        this.skippedThrottle.incrBy(stats.getRate());
    }

    public void skippedInactive(CommonStats stats) {
        this.skippedInactive.incrBy(stats.getRate());
    }
}