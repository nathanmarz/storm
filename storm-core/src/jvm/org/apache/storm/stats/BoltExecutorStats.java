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

import com.google.common.collect.Lists;

import org.apache.storm.generated.BoltStats;
import org.apache.storm.generated.ExecutorSpecificStats;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.metric.internal.MultiCountStatAndMetric;
import org.apache.storm.metric.internal.MultiLatencyStatAndMetric;

import java.util.List;

@SuppressWarnings("unchecked")
public class BoltExecutorStats extends CommonStats {

    public static final String ACKED = "acked";
    public static final String FAILED = "failed";
    public static final String EXECUTED = "executed";
    public static final String PROCESS_LATENCIES = "process-latencies";
    public static final String EXECUTE_LATENCIES = "execute-latencies";

    public BoltExecutorStats(int rate) {
        super(rate);

        this.put(ACKED, new MultiCountStatAndMetric(NUM_STAT_BUCKETS));
        this.put(FAILED, new MultiCountStatAndMetric(NUM_STAT_BUCKETS));
        this.put(EXECUTED, new MultiCountStatAndMetric(NUM_STAT_BUCKETS));
        this.put(PROCESS_LATENCIES, new MultiLatencyStatAndMetric(NUM_STAT_BUCKETS));
        this.put(EXECUTE_LATENCIES, new MultiLatencyStatAndMetric(NUM_STAT_BUCKETS));
    }

    public MultiCountStatAndMetric getAcked() {
        return (MultiCountStatAndMetric) this.get(ACKED);
    }

    public MultiCountStatAndMetric getFailed() {
        return (MultiCountStatAndMetric) this.get(FAILED);
    }

    public MultiCountStatAndMetric getExecuted() {
        return (MultiCountStatAndMetric) this.get(EXECUTED);
    }

    public MultiLatencyStatAndMetric getProcessLatencies() {
        return (MultiLatencyStatAndMetric) this.get(PROCESS_LATENCIES);
    }

    public MultiLatencyStatAndMetric getExecuteLatencies() {
        return (MultiLatencyStatAndMetric) this.get(EXECUTE_LATENCIES);
    }

    public void boltExecuteTuple(String component, String stream, long latencyMs) {
        List key = Lists.newArrayList(component, stream);
        this.getExecuted().incBy(key, this.rate);
        this.getExecuteLatencies().record(key, latencyMs);
    }

    public void boltAckedTuple(String component, String stream, long latencyMs) {
        List key = Lists.newArrayList(component, stream);
        this.getAcked().incBy(key, this.rate);
        this.getProcessLatencies().record(key, latencyMs);
    }

    public void boltFailedTuple(String component, String stream, long latencyMs) {
        List key = Lists.newArrayList(component, stream);
        this.getFailed().incBy(key, this.rate);

    }

    public ExecutorStats renderStats() {
        ExecutorStats ret = new ExecutorStats();
        // common stats
        ret.set_emitted(valueStat(EMITTED));
        ret.set_transferred(valueStat(TRANSFERRED));
        ret.set_rate(this.rate);

        // bolt stats
        BoltStats boltStats = new BoltStats(
                StatsUtil.windowSetConverter(valueStat(ACKED), StatsUtil.TO_GSID, StatsUtil.IDENTITY),
                StatsUtil.windowSetConverter(valueStat(FAILED), StatsUtil.TO_GSID, StatsUtil.IDENTITY),
                StatsUtil.windowSetConverter(valueStat(PROCESS_LATENCIES), StatsUtil.TO_GSID, StatsUtil.IDENTITY),
                StatsUtil.windowSetConverter(valueStat(EXECUTED), StatsUtil.TO_GSID, StatsUtil.IDENTITY),
                StatsUtil.windowSetConverter(valueStat(EXECUTE_LATENCIES), StatsUtil.TO_GSID, StatsUtil.IDENTITY));
        ret.set_specific(ExecutorSpecificStats.bolt(boltStats));

        return ret;
    }
}
