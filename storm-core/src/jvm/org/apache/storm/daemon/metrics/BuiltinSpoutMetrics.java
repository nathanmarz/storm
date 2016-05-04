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

import org.apache.storm.metric.internal.MultiCountStatAndMetric;
import org.apache.storm.metric.internal.MultiLatencyStatAndMetric;
import org.apache.storm.stats.SpoutExecutorStats;

public class BuiltinSpoutMetrics extends BuiltinMetrics {
    private final MultiCountStatAndMetric ackCount;
    private final MultiCountStatAndMetric failCount;
    private final MultiCountStatAndMetric emitCount;
    private final MultiCountStatAndMetric transferCount;
    private final MultiLatencyStatAndMetric completeLatency;

    public BuiltinSpoutMetrics(SpoutExecutorStats stats) {
        this.ackCount = stats.getAcked();
        this.failCount = stats.getFailed();
        this.emitCount = stats.getEmitted();
        this.transferCount = stats.getTransferred();
        this.completeLatency = stats.getCompleteLatencies();

        this.metricMap.put("ack-count", ackCount);
        this.metricMap.put("fail-count", failCount);
        this.metricMap.put("emit-count", emitCount);
        this.metricMap.put("transfer-count", transferCount);
        this.metricMap.put("complete-latency", completeLatency);
    }

    public MultiCountStatAndMetric getAckCount() {
        return ackCount;
    }

    public MultiCountStatAndMetric getFailCount() {
        return failCount;
    }

    public MultiCountStatAndMetric getEmitCount() {
        return emitCount;
    }

    public MultiCountStatAndMetric getTransferCount() {
        return transferCount;
    }

    public MultiLatencyStatAndMetric getCompleteLatency() {
        return completeLatency;
    }
}
