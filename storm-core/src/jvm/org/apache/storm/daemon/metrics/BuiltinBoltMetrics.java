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
import org.apache.storm.stats.BoltExecutorStats;

public class BuiltinBoltMetrics extends BuiltinMetrics {
    private final MultiCountStatAndMetric ackCount;
    private final MultiCountStatAndMetric failCount;
    private final MultiCountStatAndMetric emitCount;
    private final MultiCountStatAndMetric executeCount;
    private final MultiCountStatAndMetric transferCount;
    private final MultiLatencyStatAndMetric executeLatency;
    private final MultiLatencyStatAndMetric processLatency;

    public BuiltinBoltMetrics(BoltExecutorStats stats) {
        this.ackCount = stats.getAcked();
        this.failCount = stats.getFailed();
        this.emitCount = stats.getEmitted();
        this.executeCount = stats.getExecuted();
        this.transferCount = stats.getTransferred();
        this.executeLatency = stats.getExecuteLatencies();
        this.processLatency = stats.getProcessLatencies();

        this.metricMap.put("ack-count", ackCount);
        this.metricMap.put("fail-count", failCount);
        this.metricMap.put("emit-count", emitCount);
        this.metricMap.put("transfer-count", transferCount);
        this.metricMap.put("execute-count", executeCount);
        this.metricMap.put("process-latency", processLatency);
        this.metricMap.put("execute-latency", executeLatency);
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

    public MultiCountStatAndMetric getExecuteCount() {
        return executeCount;
    }

    public MultiLatencyStatAndMetric getExecuteLatency() {
        return executeLatency;
    }

    public MultiLatencyStatAndMetric getProcessLatency() {
        return processLatency;
    }
}
