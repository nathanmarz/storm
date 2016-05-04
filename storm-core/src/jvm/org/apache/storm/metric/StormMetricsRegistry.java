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

import com.codahale.metrics.*;

import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.storm.daemon.metrics.MetricsUtils;
import org.apache.storm.daemon.metrics.reporters.PreparableReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unchecked")
public class StormMetricsRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(StormMetricsRegistry.class);
    public static final MetricRegistry DEFAULT_REGISTRY = new MetricRegistry();

    public static Meter registerMeter(String name) {
        Meter meter = new Meter();
        return register(name, meter);
    }

    // TODO: should replace Callable to Gauge<Integer> when nimbus.clj is translated to java
    public static Gauge<Integer> registerGauge(final String name, final Callable fn) {
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

    public static Histogram registerHistogram(String name, Reservoir reservoir) {
        Histogram histogram = new Histogram(reservoir);
        return register(name, histogram);
    }

    public static void startMetricsReporters(Map stormConf) {
        for (PreparableReporter reporter : MetricsUtils.getPreparableReporters(stormConf)) {
            startMetricsReporter(reporter, stormConf);
        }
    }

    private static void startMetricsReporter(PreparableReporter reporter, Map stormConf) {
        reporter.prepare(StormMetricsRegistry.DEFAULT_REGISTRY, stormConf);
        reporter.start();
        LOG.info("Started statistics report plugin...");
    }

    private static <T extends Metric> T register(String name, T metric) {
        T ret;
        try {
            ret = DEFAULT_REGISTRY.register(name, metric);
        } catch (IllegalArgumentException e) {
            // swallow IllegalArgumentException when the metric exists already
            ret = (T) DEFAULT_REGISTRY.getMetrics().get(name);
            if (ret == null) {
                throw e;
            } else {
                LOG.warn("Metric {} has already been registered", name);
            }
        }
        return ret;
    }
}