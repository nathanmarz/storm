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
package org.apache.storm.metric;

import clojure.lang.IFn;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unchecked")
public class StormMetricsRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(StormMetricsRegistry.class);
    private static final MetricRegistry metrics = new MetricRegistry();

    public static Meter registerMeter(String name) {
        Meter meter = new Meter();
        return register(name, meter);
    }

    // TODO: should replace fn to Gauge<Integer> when nimbus.clj is translated to java
    public static Gauge<Integer> registerGauge(final String name, final IFn fn) {
        Gauge<Integer> gauge = new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                try {
                    return (Integer) fn.call();
                } catch (Exception e) {
                    LOG.error("Error getting gauge value for {}", name, e);
                }
                return 0;
            }
        };
        return register(name, gauge);
    }

    private static <T extends Metric> T register(String name, T metric) {
        T ret;
        try {
            ret = metrics.register(name, metric);
        } catch (IllegalArgumentException e) {
            // swallow IllegalArgumentException when the metric exists already
            ret = (T) metrics.getMetrics().get(name);
            if (ret == null) {
                throw e;
            } else {
                LOG.warn("Metric {} has already been registered", name);
            }
        }
        return ret;
    }
}