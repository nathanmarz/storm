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
package org.apache.storm.daemon.metrics.reporters;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class JmxPreparableReporter implements PreparableReporter<JmxReporter> {
    private final static Logger LOG = LoggerFactory.getLogger(JmxPreparableReporter.class);
    JmxReporter reporter = null;

    @Override
    public void prepare(MetricRegistry metricsRegistry, Map stormConf) {
        LOG.info("Preparing...");
        JmxReporter.Builder builder = JmxReporter.forRegistry(metricsRegistry);
        String domain = Utils.getString(stormConf.get(":domain"), null);
        if (domain != null) {
            builder.inDomain(domain);
        }
        String rateUnit = Utils.getString(stormConf.get(":rate-unit"), null);
        if (rateUnit != null) {
            builder.convertRatesTo(TimeUnit.valueOf(rateUnit));
        }
        MetricFilter filter = (MetricFilter) stormConf.get(":filter");
        if (filter != null) {
            builder.filter(filter);
        }
        reporter = builder.build();

    }

    @Override
    public void start() {
        if (reporter != null ) {
            LOG.info("Starting...");
            reporter.start();
        } else {
            throw new IllegalStateException("Attempt to start without preparing " + getClass().getSimpleName());
        }
    }

    @Override
    public void stop() {
        if (reporter !=null) {
            LOG.info("Stopping...");
            reporter.stop();
        } else {
            throw new IllegalStateException("Attempt to stop without preparing " + getClass().getSimpleName());
        }
    }
}
