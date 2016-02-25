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

import clojure.lang.Keyword;
import clojure.lang.RT;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.BoltAggregateStats;
import org.apache.storm.generated.BoltStats;
import org.apache.storm.generated.CommonAggregateStats;
import org.apache.storm.generated.ComponentAggregateStats;
import org.apache.storm.generated.ComponentPageInfo;
import org.apache.storm.generated.ComponentType;
import org.apache.storm.generated.ErrorInfo;
import org.apache.storm.generated.ExecutorAggregateStats;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.ExecutorSpecificStats;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.SpecificAggregateStats;
import org.apache.storm.generated.SpoutAggregateStats;
import org.apache.storm.generated.SpoutStats;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.TopologyPageInfo;
import org.apache.storm.generated.TopologyStats;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unchecked, unused")
public class StatsUtil {
    private static final Logger logger = LoggerFactory.getLogger(StatsUtil.class);

    public static final String TYPE = "type";
    private static final String SPOUT = "spout";
    private static final String BOLT = "bolt";
    public static final Keyword KW_SPOUT = keyword(SPOUT);
    public static final Keyword KW_BOLT = keyword(BOLT);

    private static final String UPTIME = "uptime";
    private static final String HOST = "host";
    private static final String PORT = "port";
    private static final String NUM_TASKS = "num-tasks";
    private static final String NUM_EXECUTORS = "num-executors";
    private static final String NUM_WORKERS = "num-workers";
    private static final String CAPACITY = "capacity";
    private static final String STATS = "stats";
    private static final String EXECUTOR_STATS = "executor-stats";
    private static final String EXECUTOR_ID = "executor-id";
    private static final String LAST_ERROR = "lastError";

    private static final String ACKED = "acked";
    private static final String FAILED = "failed";
    private static final String EXECUTED = "executed";
    private static final String EMITTED = "emitted";
    private static final String TRANSFERRED = "transferred";

    private static final String EXEC_LATENCIES = "execute-latencies";
    private static final String PROC_LATENCIES = "process-latencies";
    private static final String COMP_LATENCIES = "complete-latencies";

    private static final String EXEC_LATENCY = "execute-latency";
    private static final String PROC_LATENCY = "process-latency";
    private static final String COMP_LATENCY = "complete-latency";

    private static final String EXEC_LAT_TOTAL = "executeLatencyTotal";
    private static final String PROC_LAT_TOTAL = "processLatencyTotal";
    private static final String COMP_LAT_TOTAL = "completeLatencyTotal";

    private static final String WIN_TO_EMITTED = "window->emitted";
    private static final String WIN_TO_ACKED = "window->acked";
    private static final String WIN_TO_FAILED = "window->failed";
    private static final String WIN_TO_EXECUTED = "window->executed";
    private static final String WIN_TO_TRANSFERRED = "window->transferred";
    private static final String WIN_TO_EXEC_LAT = "window->execute-latency";
    private static final String WIN_TO_PROC_LAT = "window->process-latency";
    private static final String WIN_TO_COMP_LAT = "window->complete-latency";
    private static final String WIN_TO_COMP_LAT_WGT_AVG = "window->comp-lat-wgt-avg";
    private static final String WIN_TO_EXEC_LAT_WGT_AVG = "window->exec-lat-wgt-avg";
    private static final String WIN_TO_PROC_LAT_WGT_AVG = "window->proc-lat-wgt-avg";

    private static final String BOLT_TO_STATS = "bolt-id->stats";
    private static final String SPOUT_TO_STATS = "spout-id->stats";
    private static final String SID_TO_OUT_STATS = "sid->output-stats";
    private static final String CID_SID_TO_IN_STATS = "cid+sid->input-stats";
    private static final String WORKERS_SET = "workers-set";

    public static final int TEN_MIN_IN_SECONDS = 60 * 10;
    public static final String TEN_MIN_IN_SECONDS_STR = TEN_MIN_IN_SECONDS + "";

    private static final IdentityTransformer IDENTITY = new IdentityTransformer();
    private static final ToStringTransformer TO_STRING = new ToStringTransformer();
    private static final FromGlobalStreamIdTransformer FROM_GSID = new FromGlobalStreamIdTransformer();
    private static final ToGlobalStreamIdTransformer TO_GSID = new ToGlobalStreamIdTransformer();


    // =====================================================================================
    // aggregation stats methods
    // =====================================================================================

    /**
     * Aggregates number executed, process latency, and execute latency across all streams.
     *
     * @param id2execAvg { global stream id -> exec avg value }, e.g., {["split" "default"] 0.44313}
     * @param id2procAvg { global stream id -> proc avg value }
     * @param id2numExec { global stream id -> executed }
     */
    public static Map aggBoltLatAndCount(Map id2execAvg, Map id2procAvg, Map id2numExec) {
        Map ret = new HashMap();
        putRawKV(ret, EXEC_LAT_TOTAL, weightAvgAndSum(id2execAvg, id2numExec));
        putRawKV(ret, PROC_LAT_TOTAL, weightAvgAndSum(id2procAvg, id2numExec));
        putRawKV(ret, EXECUTED, sumValues(id2numExec));

        return ret;
    }

    /**
     * Aggregates number acked and complete latencies across all streams.
     */
    public static Map aggSpoutLatAndCount(Map id2compAvg, Map id2numAcked) {
        Map ret = new HashMap();
        putRawKV(ret, COMP_LAT_TOTAL, weightAvgAndSum(id2compAvg, id2numAcked));
        putRawKV(ret, ACKED, sumValues(id2numAcked));

        return ret;
    }

    /**
     * Aggregates number executed and process & execute latencies.
     */
    public static Map aggBoltStreamsLatAndCount(Map id2execAvg, Map id2procAvg, Map id2numExec) {
        Map ret = new HashMap();
        if (id2execAvg == null || id2procAvg == null || id2numExec == null) {
            return ret;
        }
        for (Object k : id2execAvg.keySet()) {
            Map subMap = new HashMap();
            putRawKV(subMap, EXEC_LAT_TOTAL, weightAvg(id2execAvg, id2numExec, k));
            putRawKV(subMap, PROC_LAT_TOTAL, weightAvg(id2procAvg, id2numExec, k));
            putRawKV(subMap, EXECUTED, id2numExec.get(k));
            ret.put(k, subMap);
        }
        return ret;
    }

    /**
     * Aggregates number acked and complete latencies.
     */
    public static Map aggSpoutStreamsLatAndCount(Map id2compAvg, Map id2acked) {
        Map ret = new HashMap();
        if (id2compAvg == null || id2acked == null) {
            return ret;
        }
        for (Object k : id2compAvg.keySet()) {
            Map subMap = new HashMap();
            putRawKV(subMap, COMP_LAT_TOTAL, weightAvg(id2compAvg, id2acked, k));
            putRawKV(subMap, ACKED, id2acked.get(k));
            ret.put(k, subMap);
        }
        return ret;
    }

    public static Map aggPreMergeCompPageBolt(Map m, String window, boolean includeSys) {
        Map ret = new HashMap();
        putRawKV(ret, EXECUTOR_ID, getByKeyword(m, "exec-id"));
        putRawKV(ret, HOST, getByKeyword(m, HOST));
        putRawKV(ret, PORT, getByKeyword(m, PORT));
        putRawKV(ret, UPTIME, getByKeyword(m, UPTIME));
        putRawKV(ret, NUM_EXECUTORS, 1);
        putRawKV(ret, NUM_TASKS, getByKeyword(m, NUM_TASKS));

        Map stat2win2sid2num = getMapByKeyword(m, STATS);
        putRawKV(ret, CAPACITY, computeAggCapacity(stat2win2sid2num, getByKeywordOr0(m, UPTIME).intValue()));

        // calc cid+sid->input_stats
        Map inputStats = new HashMap();
        Map sid2acked = (Map) windowSetConverter(getMapByKeyword(stat2win2sid2num, ACKED), TO_STRING).get(window);
        Map sid2failed = (Map) windowSetConverter(getMapByKeyword(stat2win2sid2num, FAILED), TO_STRING).get(window);
        putRawKV(inputStats, ACKED, sid2acked != null ? sid2acked : new HashMap());
        putRawKV(inputStats, FAILED, sid2failed != null ? sid2failed : new HashMap());

        inputStats = swapMapOrder(inputStats);

        Map sid2execLat = (Map) windowSetConverter(getMapByKeyword(stat2win2sid2num, EXEC_LATENCIES), TO_STRING).get(window);
        Map sid2procLat = (Map) windowSetConverter(getMapByKeyword(stat2win2sid2num, PROC_LATENCIES), TO_STRING).get(window);
        Map sid2exec = (Map) windowSetConverter(getMapByKeyword(stat2win2sid2num, EXECUTED), TO_STRING).get(window);
        mergeMaps(inputStats, aggBoltStreamsLatAndCount(sid2execLat, sid2procLat, sid2exec));
        putRawKV(ret, CID_SID_TO_IN_STATS, inputStats);

        // calc sid->output_stats
        Map outputStats = new HashMap();
        Map sid2emitted = (Map) windowSetConverter(getMapByKeyword(stat2win2sid2num, EMITTED), TO_STRING).get(window);
        Map sid2transferred = (Map) windowSetConverter(getMapByKeyword(stat2win2sid2num, TRANSFERRED), TO_STRING).get(window);
        if (sid2emitted != null) {
            putRawKV(outputStats, EMITTED, filterSysStreams(sid2emitted, includeSys));
        } else {
            putRawKV(outputStats, EMITTED, new HashMap());
        }
        if (sid2transferred != null) {
            putRawKV(outputStats, TRANSFERRED, filterSysStreams(sid2transferred, includeSys));
        } else {
            putRawKV(outputStats, TRANSFERRED, new HashMap());
        }
        outputStats = swapMapOrder(outputStats);
        putRawKV(ret, SID_TO_OUT_STATS, outputStats);

        return ret;
    }

    public static Map aggPreMergeCompPageSpout(Map m, String window, boolean includeSys) {
        Map ret = new HashMap();
        putRawKV(ret, EXECUTOR_ID, getByKeyword(m, "exec-id"));
        putRawKV(ret, HOST, getByKeyword(m, HOST));
        putRawKV(ret, PORT, getByKeyword(m, PORT));
        putRawKV(ret, UPTIME, getByKeyword(m, UPTIME));
        putRawKV(ret, NUM_EXECUTORS, 1);
        putRawKV(ret, NUM_TASKS, getByKeyword(m, NUM_TASKS));

        Map stat2win2sid2num = getMapByKeyword(m, STATS);

        // calc sid->output-stats
        Map outputStats = new HashMap();
        Map win2sid2acked = windowSetConverter(getMapByKeyword(stat2win2sid2num, ACKED), TO_STRING);
        Map win2sid2failed = windowSetConverter(getMapByKeyword(stat2win2sid2num, FAILED), TO_STRING);
        Map win2sid2emitted = windowSetConverter(getMapByKeyword(stat2win2sid2num, EMITTED), TO_STRING);
        Map win2sid2transferred = windowSetConverter(getMapByKeyword(stat2win2sid2num, TRANSFERRED), TO_STRING);
        Map win2sid2compLat = windowSetConverter(getMapByKeyword(stat2win2sid2num, COMP_LATENCIES), TO_STRING);

        putRawKV(outputStats, ACKED, win2sid2acked.get(window));
        putRawKV(outputStats, FAILED, win2sid2failed.get(window));
        putRawKV(outputStats, EMITTED, filterSysStreams((Map) win2sid2emitted.get(window), includeSys));
        putRawKV(outputStats, TRANSFERRED, filterSysStreams((Map) win2sid2transferred.get(window), includeSys));
        outputStats = swapMapOrder(outputStats);

        Map sid2compLat = (Map) win2sid2compLat.get(window);
        Map sid2acked = (Map) win2sid2acked.get(window);
        mergeMaps(outputStats, aggSpoutStreamsLatAndCount(sid2compLat, sid2acked));
        putRawKV(ret, SID_TO_OUT_STATS, outputStats);

        return ret;
    }

    public static Map aggPreMergeTopoPageBolt(Map m, String window, boolean includeSys) {
        Map ret = new HashMap();

        Map subRet = new HashMap();
        putRawKV(subRet, NUM_EXECUTORS, 1);
        putRawKV(subRet, NUM_TASKS, getByKeyword(m, NUM_TASKS));

        Map stat2win2sid2num = getMapByKeyword(m, STATS);
        putRawKV(subRet, CAPACITY, computeAggCapacity(stat2win2sid2num, getByKeywordOr0(m, UPTIME).intValue()));

        for (String key : new String[]{EMITTED, TRANSFERRED, ACKED, FAILED}) {
            Map stat = (Map) windowSetConverter(getMapByKeyword(stat2win2sid2num, key), TO_STRING).get(window);
            if (EMITTED.equals(key) || TRANSFERRED.equals(key)) {
                stat = filterSysStreams(stat, includeSys);
            }
            long sum = 0;
            if (stat != null) {
                for (Object o : stat.values()) {
                    sum += ((Number) o).longValue();
                }
            }
            putRawKV(subRet, key, sum);
        }

        Map win2sid2execLat = windowSetConverter(getMapByKeyword(stat2win2sid2num, EXEC_LATENCIES), TO_STRING);
        Map win2sid2procLat = windowSetConverter(getMapByKeyword(stat2win2sid2num, PROC_LATENCIES), TO_STRING);
        Map win2sid2exec = windowSetConverter(getMapByKeyword(stat2win2sid2num, EXECUTED), TO_STRING);
        subRet.putAll(aggBoltLatAndCount(
                (Map) win2sid2execLat.get(window), (Map) win2sid2procLat.get(window), (Map) win2sid2exec.get(window)));

        ret.put(getByKeyword(m, "comp-id"), subRet);
        return ret;
    }

    public static Map aggPreMergeTopoPageSpout(Map m, String window, boolean includeSys) {
        Map ret = new HashMap();

        Map subRet = new HashMap();
        putRawKV(subRet, NUM_EXECUTORS, 1);
        putRawKV(subRet, NUM_TASKS, getByKeyword(m, NUM_TASKS));

        // no capacity for spout
        Map stat2win2sid2num = getMapByKeyword(m, STATS);
        for (String key : new String[]{EMITTED, TRANSFERRED, FAILED}) {
            Map stat = (Map) windowSetConverter(getMapByKeyword(stat2win2sid2num, key), TO_STRING).get(window);
            if (EMITTED.equals(key) || TRANSFERRED.equals(key)) {
                stat = filterSysStreams(stat, includeSys);
            }
            long sum = 0;
            if (stat != null) {
                for (Object o : stat.values()) {
                    sum += ((Number) o).longValue();
                }
            }
            putRawKV(subRet, key, sum);
        }

        Map win2sid2compLat = windowSetConverter(getMapByKeyword(stat2win2sid2num, COMP_LATENCIES), TO_STRING);
        Map win2sid2acked = windowSetConverter(getMapByKeyword(stat2win2sid2num, ACKED), TO_STRING);
        subRet.putAll(aggSpoutLatAndCount((Map) win2sid2compLat.get(window), (Map) win2sid2acked.get(window)));

        ret.put(getByKeyword(m, "comp-id"), subRet);
        return ret;
    }

    public static Map mergeAggCompStatsCompPageBolt(Map accBoltStats, Map boltStats) {
        Map ret = new HashMap();

        Map accIn = getMapByKeyword(accBoltStats, CID_SID_TO_IN_STATS);
        Map accOut = getMapByKeyword(accBoltStats, SID_TO_OUT_STATS);
        Map boltIn = getMapByKeyword(boltStats, CID_SID_TO_IN_STATS);
        Map boltOut = getMapByKeyword(boltStats, SID_TO_OUT_STATS);

        int numExecutors = getByKeywordOr0(accBoltStats, NUM_EXECUTORS).intValue();
        putRawKV(ret, NUM_EXECUTORS, numExecutors + 1);
        putRawKV(ret, NUM_TASKS, sumOr0(
                getByKeywordOr0(accBoltStats, NUM_TASKS), getByKeywordOr0(boltStats, NUM_TASKS)));

        // (merge-with (partial merge-with sum-or-0) acc-out spout-out)
        putRawKV(ret, SID_TO_OUT_STATS, fullMergeWithSum(accOut, boltOut));
        putRawKV(ret, CID_SID_TO_IN_STATS, fullMergeWithSum(accIn, boltIn));

        long executed = sumStreamsLong(boltIn, EXECUTED);
        putRawKV(ret, EXECUTED, executed);

        Map executorStats = new HashMap();
        putRawKV(executorStats, EXECUTOR_ID, getByKeyword(boltStats, EXECUTOR_ID));
        putRawKV(executorStats, UPTIME, getByKeyword(boltStats, UPTIME));
        putRawKV(executorStats, HOST, getByKeyword(boltStats, HOST));
        putRawKV(executorStats, PORT, getByKeyword(boltStats, PORT));
        putRawKV(executorStats, CAPACITY, getByKeyword(boltStats, CAPACITY));

        putRawKV(executorStats, EMITTED, sumStreamsLong(boltOut, EMITTED));
        putRawKV(executorStats, TRANSFERRED, sumStreamsLong(boltOut, TRANSFERRED));
        putRawKV(executorStats, ACKED, sumStreamsLong(boltIn, ACKED));
        putRawKV(executorStats, FAILED, sumStreamsLong(boltIn, FAILED));
        putRawKV(executorStats, EXECUTED, executed);

        if (executed > 0) {
            putRawKV(executorStats, EXEC_LATENCY, sumStreamsDouble(boltIn, EXEC_LAT_TOTAL) / executed);
            putRawKV(executorStats, PROC_LATENCY, sumStreamsDouble(boltIn, PROC_LAT_TOTAL) / executed);
        } else {
            putRawKV(executorStats, EXEC_LATENCY, null);
            putRawKV(executorStats, PROC_LATENCY, null);
        }
        List executorStatsList = ((List) getByKeyword(accBoltStats, EXECUTOR_STATS));
        executorStatsList.add(executorStats);
        putRawKV(ret, EXECUTOR_STATS, executorStatsList);

        return ret;
    }

    public static Map mergeAggCompStatsCompPageSpout(Map accSpoutStats, Map spoutStats) {
        Map ret = new HashMap();

        Map accOut = getMapByKeyword(accSpoutStats, SID_TO_OUT_STATS);
        Map spoutOut = getMapByKeyword(spoutStats, SID_TO_OUT_STATS);

        int numExecutors = getByKeywordOr0(accSpoutStats, NUM_EXECUTORS).intValue();
        putRawKV(ret, NUM_EXECUTORS, numExecutors + 1);
        putRawKV(ret, NUM_TASKS, sumOr0(
                getByKeywordOr0(accSpoutStats, NUM_TASKS), getByKeywordOr0(spoutStats, NUM_TASKS)));
        putRawKV(ret, SID_TO_OUT_STATS, fullMergeWithSum(accOut, spoutOut));

        Map executorStats = new HashMap();
        putRawKV(executorStats, EXECUTOR_ID, getByKeyword(spoutStats, EXECUTOR_ID));
        putRawKV(executorStats, UPTIME, getByKeyword(spoutStats, UPTIME));
        putRawKV(executorStats, HOST, getByKeyword(spoutStats, HOST));
        putRawKV(executorStats, PORT, getByKeyword(spoutStats, PORT));

        putRawKV(executorStats, EMITTED, sumStreamsLong(spoutOut, EMITTED));
        putRawKV(executorStats, TRANSFERRED, sumStreamsLong(spoutOut, TRANSFERRED));
        putRawKV(executorStats, FAILED, sumStreamsLong(spoutOut, FAILED));
        long acked = sumStreamsLong(spoutOut, ACKED);
        putRawKV(executorStats, ACKED, acked);
        if (acked > 0) {
            putRawKV(executorStats, COMP_LATENCY, sumStreamsDouble(spoutOut, COMP_LAT_TOTAL) / acked);
        } else {
            putRawKV(executorStats, COMP_LATENCY, null);
        }
        List executorStatsList = ((List) getByKeyword(accSpoutStats, EXECUTOR_STATS));
        executorStatsList.add(executorStats);
        putRawKV(ret, EXECUTOR_STATS, executorStatsList);

        return ret;
    }

    public static Map mergeAggCompStatsTopoPageBolt(Map accBoltStats, Map boltStats) {
        Map ret = new HashMap();
        Integer numExecutors = getByKeywordOr0(accBoltStats, NUM_EXECUTORS).intValue();
        putRawKV(ret, NUM_EXECUTORS, numExecutors + 1);
        putRawKV(ret, NUM_TASKS, sumOr0(
                getByKeywordOr0(accBoltStats, NUM_TASKS), getByKeywordOr0(boltStats, NUM_TASKS)));
        putRawKV(ret, EMITTED, sumOr0(
                getByKeywordOr0(accBoltStats, EMITTED), getByKeywordOr0(boltStats, EMITTED)));
        putRawKV(ret, TRANSFERRED, sumOr0(
                getByKeywordOr0(accBoltStats, TRANSFERRED), getByKeywordOr0(boltStats, TRANSFERRED)));
        putRawKV(ret, EXEC_LAT_TOTAL, sumOr0(
                getByKeywordOr0(accBoltStats, EXEC_LAT_TOTAL), getByKeywordOr0(boltStats, EXEC_LAT_TOTAL)));
        putRawKV(ret, PROC_LAT_TOTAL, sumOr0(
                getByKeywordOr0(accBoltStats, PROC_LAT_TOTAL), getByKeywordOr0(boltStats, PROC_LAT_TOTAL)));
        putRawKV(ret, EXECUTED, sumOr0(
                getByKeywordOr0(accBoltStats, EXECUTED), getByKeywordOr0(boltStats, EXECUTED)));
        putRawKV(ret, ACKED, sumOr0(
                getByKeywordOr0(accBoltStats, ACKED), getByKeywordOr0(boltStats, ACKED)));
        putRawKV(ret, FAILED, sumOr0(
                getByKeywordOr0(accBoltStats, FAILED), getByKeywordOr0(boltStats, FAILED)));
        putRawKV(ret, CAPACITY, maxOr0(
                getByKeywordOr0(accBoltStats, CAPACITY), getByKeywordOr0(boltStats, CAPACITY)));

        return ret;
    }

    public static Map mergeAggCompStatsTopoPageSpout(Map accSpoutStats, Map spoutStats) {
        Map ret = new HashMap();
        Integer numExecutors = getByKeywordOr0(accSpoutStats, NUM_EXECUTORS).intValue();
        putRawKV(ret, NUM_EXECUTORS, numExecutors + 1);
        putRawKV(ret, NUM_TASKS, sumOr0(
                getByKeywordOr0(accSpoutStats, NUM_TASKS), getByKeywordOr0(spoutStats, NUM_TASKS)));
        putRawKV(ret, EMITTED, sumOr0(
                getByKeywordOr0(accSpoutStats, EMITTED), getByKeywordOr0(spoutStats, EMITTED)));
        putRawKV(ret, TRANSFERRED, sumOr0(
                getByKeywordOr0(accSpoutStats, TRANSFERRED), getByKeywordOr0(spoutStats, TRANSFERRED)));
        putRawKV(ret, COMP_LAT_TOTAL, sumOr0(
                getByKeywordOr0(accSpoutStats, COMP_LAT_TOTAL), getByKeywordOr0(spoutStats, COMP_LAT_TOTAL)));
        putRawKV(ret, ACKED, sumOr0(
                getByKeywordOr0(accSpoutStats, ACKED), getByKeywordOr0(spoutStats, ACKED)));
        putRawKV(ret, FAILED, sumOr0(
                getByKeywordOr0(accSpoutStats, FAILED), getByKeywordOr0(spoutStats, FAILED)));

        return ret;
    }

    /**
     * A helper function that does the common work to aggregate stats of one
     * executor with the given map for the topology page.
     */
    public static Map aggTopoExecStats(String window, boolean includeSys, Map accStats, Map newData, String compType) {
        Map ret = new HashMap();

        Set workerSet = (Set) getByKeyword(accStats, WORKERS_SET);
        Map bolt2stats = getMapByKeyword(accStats, BOLT_TO_STATS);
        Map spout2stats = getMapByKeyword(accStats, SPOUT_TO_STATS);
        Map win2emitted = getMapByKeyword(accStats, WIN_TO_EMITTED);
        Map win2transferred = getMapByKeyword(accStats, WIN_TO_TRANSFERRED);
        Map win2compLatWgtAvg = getMapByKeyword(accStats, WIN_TO_COMP_LAT_WGT_AVG);
        Map win2acked = getMapByKeyword(accStats, WIN_TO_ACKED);
        Map win2failed = getMapByKeyword(accStats, WIN_TO_FAILED);
        Map stats = getMapByKeyword(newData, STATS);

        boolean isSpout = compType.equals(SPOUT);
        Map cid2stat2num;
        if (isSpout) {
            cid2stat2num = aggPreMergeTopoPageSpout(newData, window, includeSys);
        } else {
            cid2stat2num = aggPreMergeTopoPageBolt(newData, window, includeSys);
        }

        Map w2compLatWgtAvg, w2acked;
        Map compLatStats = getMapByKeyword(stats, COMP_LATENCIES);
        if (isSpout) { // agg spout stats
            Map mm = new HashMap();

            Map acked = getMapByKeyword(stats, ACKED);
            for (Object win : acked.keySet()) {
                mm.put(win, aggSpoutLatAndCount((Map) compLatStats.get(win), (Map) acked.get(win)));
            }
            mm = swapMapOrder(mm);
            w2compLatWgtAvg = getMapByKeyword(mm, COMP_LAT_TOTAL);
            w2acked = getMapByKeyword(mm, ACKED);
        } else {
            w2compLatWgtAvg = null;
            w2acked = aggregateCountStreams(getMapByKeyword(stats, ACKED));
        }

        workerSet.add(Lists.newArrayList(getByKeyword(newData, HOST), getByKeyword(newData, PORT)));
        putRawKV(ret, WORKERS_SET, workerSet);
        putRawKV(ret, BOLT_TO_STATS, bolt2stats);
        putRawKV(ret, SPOUT_TO_STATS, spout2stats);
        putRawKV(ret, WIN_TO_EMITTED, mergeWithSum(win2emitted, aggregateCountStreams(
                filterSysStreams(getMapByKeyword(stats, EMITTED), includeSys))));
        putRawKV(ret, WIN_TO_TRANSFERRED, mergeWithSum(win2transferred, aggregateCountStreams(
                filterSysStreams(getMapByKeyword(stats, TRANSFERRED), includeSys))));
        putRawKV(ret, WIN_TO_COMP_LAT_WGT_AVG, mergeWithSum(win2compLatWgtAvg, w2compLatWgtAvg));

        //boolean isSpoutStat = SPOUT.equals(((Keyword) getByKeyword(stats, TYPE)).getName());
        putRawKV(ret, WIN_TO_ACKED, isSpout ? mergeWithSum(win2acked, w2acked) : win2acked);
        putRawKV(ret, WIN_TO_FAILED, isSpout ?
                mergeWithSum(aggregateCountStreams(getMapByKeyword(stats, FAILED)), win2failed) : win2failed);
        putRawKV(ret, TYPE, getByKeyword(stats, TYPE));

        // (merge-with merge-agg-comp-stats-topo-page-bolt/spout (acc-stats comp-key) cid->statk->num)
        // (acc-stats comp-key) ==> bolt2stats/spout2stats
        if (isSpout) {
            Set<Object> keySet = new HashSet<>();
            keySet.addAll(spout2stats.keySet());
            keySet.addAll(cid2stat2num.keySet());

            Map mm = new HashMap();
            for (Object k : keySet) {
                mm.put(k, mergeAggCompStatsTopoPageSpout((Map) spout2stats.get(k), (Map) cid2stat2num.get(k)));
            }
            putRawKV(ret, SPOUT_TO_STATS, mm);
        } else {
            Set<Object> keySet = new HashSet<>();
            keySet.addAll(bolt2stats.keySet());
            keySet.addAll(cid2stat2num.keySet());

            Map mm = new HashMap();
            for (Object k : keySet) {
                mm.put(k, mergeAggCompStatsTopoPageBolt((Map) bolt2stats.get(k), (Map) cid2stat2num.get(k)));
            }
            putRawKV(ret, BOLT_TO_STATS, mm);
        }

        return ret;
    }

    public static TopologyPageInfo aggTopoExecsStats(
            String topologyId, Map exec2nodePort, Map task2component,
            Map beats, StormTopology topology, String window, boolean includeSys, IStormClusterState clusterState) {
        List beatList = extractDataFromHb(exec2nodePort, task2component, beats, includeSys, topology);
        Map topoStats = aggregateTopoStats(window, includeSys, beatList);
        topoStats = postAggregateTopoStats(task2component, exec2nodePort, topoStats, topologyId, clusterState);

        return thriftifyTopoPageData(topologyId, topoStats);
    }

    public static Map aggregateTopoStats(String win, boolean includeSys, List data) {
        Map initVal = new HashMap();
        putRawKV(initVal, WORKERS_SET, new HashSet());
        putRawKV(initVal, BOLT_TO_STATS, new HashMap());
        putRawKV(initVal, SPOUT_TO_STATS, new HashMap());
        putRawKV(initVal, WIN_TO_EMITTED, new HashMap());
        putRawKV(initVal, WIN_TO_TRANSFERRED, new HashMap());
        putRawKV(initVal, WIN_TO_COMP_LAT_WGT_AVG, new HashMap());
        putRawKV(initVal, WIN_TO_ACKED, new HashMap());
        putRawKV(initVal, WIN_TO_FAILED, new HashMap());

        for (Object o : data) {
            Map newData = (Map) o;
            String compType = ((Keyword) getByKeyword(newData, TYPE)).getName();
            initVal = aggTopoExecStats(win, includeSys, initVal, newData, compType);
        }

        return initVal;
    }

    public static Map postAggregateTopoStats(
            Map task2comp, Map exec2nodePort, Map accData, String topologyId, IStormClusterState clusterState) {
        Map ret = new HashMap();
        putRawKV(ret, NUM_TASKS, task2comp.size());
        putRawKV(ret, NUM_WORKERS, ((Set) getByKeyword(accData, WORKERS_SET)).size());
        putRawKV(ret, NUM_EXECUTORS, exec2nodePort.size());

        Map bolt2stats = getMapByKeyword(accData, BOLT_TO_STATS);
        Map aggBolt2stats = new HashMap();
        for (Object o : bolt2stats.entrySet()) {
            Map.Entry e = (Map.Entry) o;
            String id = (String) e.getKey();
            Map m = (Map) e.getValue();
            long executed = getByKeywordOr0(m, EXECUTED).longValue();
            if (executed > 0) {
                double execLatencyTotal = getByKeywordOr0(m, EXEC_LAT_TOTAL).doubleValue();
                putRawKV(m, EXEC_LATENCY, execLatencyTotal / executed);

                double procLatencyTotal = getByKeywordOr0(m, PROC_LAT_TOTAL).doubleValue();
                putRawKV(m, PROC_LATENCY, procLatencyTotal / executed);
            }
            removeByKeyword(m, EXEC_LAT_TOTAL);
            removeByKeyword(m, PROC_LAT_TOTAL);
            putRawKV(m, "last-error", getLastError(clusterState, topologyId, id));

            aggBolt2stats.put(id, m);
        }
        putRawKV(ret, BOLT_TO_STATS, aggBolt2stats);

        Map spout2stats = getMapByKeyword(accData, SPOUT_TO_STATS);
        Map spoutBolt2stats = new HashMap();
        for (Object o : spout2stats.entrySet()) {
            Map.Entry e = (Map.Entry) o;
            String id = (String) e.getKey();
            Map m = (Map) e.getValue();
            long acked = getByKeywordOr0(m, ACKED).longValue();
            if (acked > 0) {
                double compLatencyTotal = getByKeywordOr0(m, COMP_LAT_TOTAL).doubleValue();
                putRawKV(m, COMP_LATENCY, compLatencyTotal / acked);
            }
            removeByKeyword(m, COMP_LAT_TOTAL);
            putRawKV(m, "last-error", getLastError(clusterState, topologyId, id));

            spoutBolt2stats.put(id, m);
        }
        putRawKV(ret, SPOUT_TO_STATS, spoutBolt2stats);

        putRawKV(ret, WIN_TO_EMITTED, mapKeyStr(getMapByKeyword(accData, WIN_TO_EMITTED)));
        putRawKV(ret, WIN_TO_TRANSFERRED, mapKeyStr(getMapByKeyword(accData, WIN_TO_TRANSFERRED)));
        putRawKV(ret, WIN_TO_ACKED, mapKeyStr(getMapByKeyword(accData, WIN_TO_ACKED)));
        putRawKV(ret, WIN_TO_FAILED, mapKeyStr(getMapByKeyword(accData, WIN_TO_FAILED)));
        putRawKV(ret, WIN_TO_COMP_LAT, computeWeightedAveragesPerWindow(
                accData, WIN_TO_COMP_LAT_WGT_AVG, WIN_TO_ACKED));
        return ret;
    }

    /**
     * aggregate bolt stats
     *
     * @param statsSeq   a seq of ExecutorStats
     * @param includeSys whether to include system streams
     * @return aggregated bolt stats
     */
    public static Map aggregateBoltStats(List statsSeq, boolean includeSys) {
        Map ret = new HashMap();

        Map commonStats = preProcessStreamSummary(aggregateCommonStats(statsSeq), includeSys);
        List acked = new ArrayList();
        List failed = new ArrayList();
        List executed = new ArrayList();
        List processLatencies = new ArrayList();
        List executeLatencies = new ArrayList();
        for (Object o : statsSeq) {
            ExecutorStats stat = (ExecutorStats) o;
            acked.add(stat.get_specific().get_bolt().get_acked());
            failed.add(stat.get_specific().get_bolt().get_failed());
            executed.add(stat.get_specific().get_bolt().get_executed());
            processLatencies.add(stat.get_specific().get_bolt().get_process_ms_avg());
            executeLatencies.add(stat.get_specific().get_bolt().get_execute_ms_avg());
        }
        mergeMaps(ret, commonStats);
        putRawKV(ret, ACKED, aggregateCounts(acked));
        putRawKV(ret, FAILED, aggregateCounts(failed));
        putRawKV(ret, EXECUTED, aggregateCounts(executed));
        putRawKV(ret, PROC_LATENCIES, aggregateAverages(processLatencies, acked));
        putRawKV(ret, EXEC_LATENCIES, aggregateAverages(executeLatencies, executed));

        return ret;
    }

    /**
     * aggregate spout stats
     *
     * @param statsSeq   a seq of ExecutorStats
     * @param includeSys whether to include system streams
     * @return aggregated spout stats
     */
    public static Map aggregateSpoutStats(List statsSeq, boolean includeSys) {
        Map ret = new HashMap();

        Map commonStats = preProcessStreamSummary(aggregateCommonStats(statsSeq), includeSys);
        List acked = new ArrayList();
        List failed = new ArrayList();
        List completeLatencies = new ArrayList();
        for (Object o : statsSeq) {
            ExecutorStats stat = (ExecutorStats) o;
            acked.add(stat.get_specific().get_spout().get_acked());
            failed.add(stat.get_specific().get_spout().get_failed());
            completeLatencies.add(stat.get_specific().get_spout().get_complete_ms_avg());
        }
        mergeMaps(ret, commonStats);
        putRawKV(ret, ACKED, aggregateCounts(acked));
        putRawKV(ret, FAILED, aggregateCounts(failed));
        putRawKV(ret, COMP_LATENCIES, aggregateAverages(completeLatencies, acked));

        return ret;
    }

    public static Map aggregateCommonStats(List statsSeq) {
        Map ret = new HashMap();

        List emitted = new ArrayList();
        List transferred = new ArrayList();
        for (Object o : statsSeq) {
            ExecutorStats stat = (ExecutorStats) o;
            emitted.add(stat.get_emitted());
            transferred.add(stat.get_transferred());
        }

        putRawKV(ret, EMITTED, aggregateCounts(emitted));
        putRawKV(ret, TRANSFERRED, aggregateCounts(transferred));
        return ret;
    }

    public static Map preProcessStreamSummary(Map streamSummary, boolean includeSys) {
        Map emitted = getMapByKeyword(streamSummary, EMITTED);
        Map transferred = getMapByKeyword(streamSummary, TRANSFERRED);

        putRawKV(streamSummary, EMITTED, filterSysStreams(emitted, includeSys));
        putRawKV(streamSummary, TRANSFERRED, filterSysStreams(transferred, includeSys));

        return streamSummary;
    }

    public static Map aggregateCountStreams(Map stats) {
        Map ret = new HashMap();
        for (Object o : stats.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            Map value = (Map) entry.getValue();
            long sum = 0l;
            for (Object num : value.values()) {
                sum += ((Number) num).longValue();
            }
            ret.put(entry.getKey(), sum);
        }
        return ret;
    }

    public static Map aggregateAverages(List avgSeq, List countSeq) {
        Map ret = new HashMap();

        Map expands = expandAveragesSeq(avgSeq, countSeq);
        for (Object o : expands.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            Object k = entry.getKey();

            Map tmp = new HashMap();
            Map inner = (Map) entry.getValue();
            for (Object kk : inner.keySet()) {
                List vv = (List) inner.get(kk);
                tmp.put(kk, valAvg(((Number) vv.get(0)).doubleValue(), ((Number) vv.get(1)).longValue()));
            }
            ret.put(k, tmp);
        }

        return ret;
    }

    public static Map aggregateAvgStreams(Map avgs, Map counts) {
        Map ret = new HashMap();

        Map expands = expandAverages(avgs, counts);
        for (Object o : expands.entrySet()) {
            Map.Entry e = (Map.Entry) o;
            Object win = e.getKey();

            double avgTotal = 0.0;
            long cntTotal = 0l;
            Map inner = (Map) e.getValue();
            for (Object kk : inner.keySet()) {
                List vv = (List) inner.get(kk);
                avgTotal += ((Number) vv.get(0)).doubleValue();
                cntTotal += ((Number) vv.get(1)).longValue();
            }
            ret.put(win, valAvg(avgTotal, cntTotal));
        }

        return ret;
    }

    public static Map spoutStreamsStats(List summs, boolean includeSys) {
        List statsSeq = getFilledStats(summs);
        return aggregateSpoutStreams(aggregateSpoutStats(statsSeq, includeSys));
    }

    public static Map boltStreamsStats(List summs, boolean includeSys) {
        List statsSeq = getFilledStats(summs);
        return aggregateBoltStreams(aggregateBoltStats(statsSeq, includeSys));
    }

    public static Map aggregateSpoutStreams(Map stats) {
        Map ret = new HashMap();
        putRawKV(ret, ACKED, aggregateCountStreams(getMapByKeyword(stats, ACKED)));
        putRawKV(ret, FAILED, aggregateCountStreams(getMapByKeyword(stats, FAILED)));
        putRawKV(ret, EMITTED, aggregateCountStreams(getMapByKeyword(stats, EMITTED)));
        putRawKV(ret, TRANSFERRED, aggregateCountStreams(getMapByKeyword(stats, TRANSFERRED)));
        putRawKV(ret, COMP_LATENCIES, aggregateAvgStreams(
                getMapByKeyword(stats, COMP_LATENCIES), getMapByKeyword(stats, ACKED)));
        return ret;
    }

    public static Map aggregateBoltStreams(Map stats) {
        Map ret = new HashMap();
        putRawKV(ret, ACKED, aggregateCountStreams(getMapByKeyword(stats, ACKED)));
        putRawKV(ret, FAILED, aggregateCountStreams(getMapByKeyword(stats, FAILED)));
        putRawKV(ret, EMITTED, aggregateCountStreams(getMapByKeyword(stats, EMITTED)));
        putRawKV(ret, TRANSFERRED, aggregateCountStreams(getMapByKeyword(stats, TRANSFERRED)));
        putRawKV(ret, EXECUTED, aggregateCountStreams(getMapByKeyword(stats, EXECUTED)));
        putRawKV(ret, PROC_LATENCIES, aggregateAvgStreams(
                getMapByKeyword(stats, PROC_LATENCIES), getMapByKeyword(stats, ACKED)));
        putRawKV(ret, EXEC_LATENCIES, aggregateAvgStreams(
                getMapByKeyword(stats, EXEC_LATENCIES), getMapByKeyword(stats, EXECUTED)));
        return ret;
    }

    /**
     * A helper function that aggregates windowed stats from one spout executor.
     */
    public static Map aggBoltExecWinStats(Map accStats, Map newStats, boolean includeSys) {
        Map ret = new HashMap();

        Map m = new HashMap();
        for (Object win : getMapByKeyword(newStats, EXECUTED).keySet()) {
            m.put(win, aggBoltLatAndCount(
                    (Map) (getMapByKeyword(newStats, EXEC_LATENCIES)).get(win),
                    (Map) (getMapByKeyword(newStats, PROC_LATENCIES)).get(win),
                    (Map) (getMapByKeyword(newStats, EXECUTED)).get(win)));
        }
        m = swapMapOrder(m);

        Map win2execLatWgtAvg = getMapByKeyword(m, EXEC_LAT_TOTAL);
        Map win2procLatWgtAvg = getMapByKeyword(m, PROC_LAT_TOTAL);
        Map win2executed = getMapByKeyword(m, EXECUTED);

        Map emitted = getMapByKeyword(newStats, EMITTED);
        emitted = mergeWithSum(aggregateCountStreams(filterSysStreams(emitted, includeSys)),
                getMapByKeyword(accStats, WIN_TO_EMITTED));
        putRawKV(ret, WIN_TO_EMITTED, emitted);

        Map transferred = getMapByKeyword(newStats, TRANSFERRED);
        transferred = mergeWithSum(aggregateCountStreams(filterSysStreams(transferred, includeSys)),
                getMapByKeyword(accStats, WIN_TO_TRANSFERRED));
        putRawKV(ret, WIN_TO_TRANSFERRED, transferred);

        putRawKV(ret, WIN_TO_EXEC_LAT_WGT_AVG, mergeWithSum(
                getMapByKeyword(accStats, WIN_TO_EXEC_LAT_WGT_AVG), win2execLatWgtAvg));
        putRawKV(ret, WIN_TO_PROC_LAT_WGT_AVG, mergeWithSum(
                getMapByKeyword(accStats, WIN_TO_PROC_LAT_WGT_AVG), win2procLatWgtAvg));
        putRawKV(ret, WIN_TO_EXECUTED, mergeWithSum(
                getMapByKeyword(accStats, WIN_TO_EXECUTED), win2executed));
        putRawKV(ret, WIN_TO_ACKED, mergeWithSum(
                aggregateCountStreams(getMapByKeyword(newStats, ACKED)), getMapByKeyword(accStats, WIN_TO_ACKED)));
        putRawKV(ret, WIN_TO_FAILED, mergeWithSum(
                aggregateCountStreams(getMapByKeyword(newStats, FAILED)), getMapByKeyword(accStats, WIN_TO_FAILED)));

        return ret;
    }

    /**
     * A helper function that aggregates windowed stats from one spout executor.
     */
    public static Map aggSpoutExecWinStats(Map accStats, Map newStats, boolean includeSys) {
        Map ret = new HashMap();

        Map m = new HashMap();
        for (Object win : getMapByKeyword(newStats, ACKED).keySet()) {
            m.put(win, aggSpoutLatAndCount(
                    (Map) (getMapByKeyword(newStats, COMP_LATENCIES)).get(win),
                    (Map) (getMapByKeyword(newStats, ACKED)).get(win)));
        }
        m = swapMapOrder(m);

        Map win2compLatWgtAvg = getMapByKeyword(m, COMP_LAT_TOTAL);
        Map win2acked = getMapByKeyword(m, ACKED);

        Map emitted = getMapByKeyword(newStats, EMITTED);
        emitted = mergeWithSum(aggregateCountStreams(filterSysStreams(emitted, includeSys)),
                getMapByKeyword(accStats, WIN_TO_EMITTED));
        putRawKV(ret, WIN_TO_EMITTED, emitted);

        Map transferred = getMapByKeyword(newStats, TRANSFERRED);
        transferred = mergeWithSum(aggregateCountStreams(filterSysStreams(transferred, includeSys)),
                getMapByKeyword(accStats, WIN_TO_TRANSFERRED));
        putRawKV(ret, WIN_TO_TRANSFERRED, transferred);

        putRawKV(ret, WIN_TO_COMP_LAT_WGT_AVG, mergeWithSum(
                getMapByKeyword(accStats, WIN_TO_COMP_LAT_WGT_AVG), win2compLatWgtAvg));
        putRawKV(ret, WIN_TO_ACKED, mergeWithSum(
                getMapByKeyword(accStats, WIN_TO_ACKED), win2acked));
        putRawKV(ret, WIN_TO_FAILED, mergeWithSum(
                aggregateCountStreams(getMapByKeyword(newStats, FAILED)), getMapByKeyword(accStats, WIN_TO_FAILED)));

        return ret;
    }


    /**
     * aggregate counts
     *
     * @param countsSeq a seq of {win -> GlobalStreamId -> value}
     */
    public static Map aggregateCounts(List countsSeq) {
        Map ret = new HashMap();
        for (Object counts : countsSeq) {
            for (Object o : ((Map) counts).entrySet()) {
                Map.Entry e = (Map.Entry) o;
                Object win = e.getKey();
                Map stream2count = (Map) e.getValue();

                if (!ret.containsKey(win)) {
                    ret.put(win, stream2count);
                } else {
                    Map existing = (Map) ret.get(win);
                    for (Object oo : stream2count.entrySet()) {
                        Map.Entry ee = (Map.Entry) oo;
                        Object stream = ee.getKey();
                        if (!existing.containsKey(stream)) {
                            existing.put(stream, ee.getValue());
                        } else {
                            existing.put(stream, (Long) ee.getValue() + (Long) existing.get(stream));
                        }
                    }
                }
            }
        }
        return ret;
    }

    public static Map aggregateCompStats(String window, boolean includeSys, List data, String compType) {
        boolean isSpout = SPOUT.equals(compType);

        Map initVal = new HashMap();
        putRawKV(initVal, WIN_TO_ACKED, new HashMap());
        putRawKV(initVal, WIN_TO_FAILED, new HashMap());
        putRawKV(initVal, WIN_TO_EMITTED, new HashMap());
        putRawKV(initVal, WIN_TO_TRANSFERRED, new HashMap());

        Map stats = new HashMap();
        putRawKV(stats, EXECUTOR_STATS, new ArrayList());
        putRawKV(stats, SID_TO_OUT_STATS, new HashMap());
        if (isSpout) {
            putRawKV(initVal, TYPE, KW_SPOUT);
            putRawKV(initVal, WIN_TO_COMP_LAT_WGT_AVG, new HashMap());
        } else {
            putRawKV(initVal, TYPE, KW_BOLT);
            putRawKV(initVal, WIN_TO_EXECUTED, new HashMap());
            putRawKV(stats, CID_SID_TO_IN_STATS, new HashMap());
            putRawKV(initVal, WIN_TO_EXEC_LAT_WGT_AVG, new HashMap());
            putRawKV(initVal, WIN_TO_PROC_LAT_WGT_AVG, new HashMap());
        }
        putRawKV(initVal, STATS, stats);

        for (Object o : data) {
            initVal = aggCompExecStats(window, includeSys, initVal, (Map) o, compType);
        }

        return initVal;
    }

    /**
     * Combines the aggregate stats of one executor with the given map, selecting
     * the appropriate window and including system components as specified.
     */
    public static Map aggCompExecStats(String window, boolean includeSys, Map accStats, Map newData, String compType) {
        Map ret = new HashMap();
        if (SPOUT.equals(compType)) {
            ret.putAll(aggSpoutExecWinStats(accStats, getMapByKeyword(newData, STATS), includeSys));
            putRawKV(ret, STATS, mergeAggCompStatsCompPageSpout(
                    getMapByKeyword(accStats, STATS),
                    aggPreMergeCompPageSpout(newData, window, includeSys)));
        } else {
            ret.putAll(aggBoltExecWinStats(accStats, getMapByKeyword(newData, STATS), includeSys));
            putRawKV(ret, STATS, mergeAggCompStatsCompPageBolt(
                    getMapByKeyword(accStats, STATS),
                    aggPreMergeCompPageBolt(newData, window, includeSys)));
        }
        putRawKV(ret, TYPE, keyword(compType));

        return ret;
    }

    public static Map postAggregateCompStats(Map task2component, Map exec2hostPort, Map accData) {
        Map ret = new HashMap();

        String compType = ((Keyword) getByKeyword(accData, TYPE)).getName();
        Map stats = getMapByKeyword(accData, STATS);
        Integer numTasks = getByKeywordOr0(stats, NUM_TASKS).intValue();
        Integer numExecutors = getByKeywordOr0(stats, NUM_EXECUTORS).intValue();
        Map outStats = getMapByKeyword(stats, SID_TO_OUT_STATS);

        putRawKV(ret, TYPE, keyword(compType));
        putRawKV(ret, NUM_TASKS, numTasks);
        putRawKV(ret, NUM_EXECUTORS, numExecutors);
        putRawKV(ret, EXECUTOR_STATS, getByKeyword(stats, EXECUTOR_STATS));
        putRawKV(ret, WIN_TO_EMITTED, mapKeyStr(getMapByKeyword(accData, WIN_TO_EMITTED)));
        putRawKV(ret, WIN_TO_TRANSFERRED, mapKeyStr(getMapByKeyword(accData, WIN_TO_TRANSFERRED)));
        putRawKV(ret, WIN_TO_ACKED, mapKeyStr(getMapByKeyword(accData, WIN_TO_ACKED)));
        putRawKV(ret, WIN_TO_FAILED, mapKeyStr(getMapByKeyword(accData, WIN_TO_FAILED)));

        if (BOLT.equals(compType)) {
            Map inStats = getMapByKeyword(stats, CID_SID_TO_IN_STATS);

            Map inStats2 = new HashMap();
            for (Object o : inStats.entrySet()) {
                Map.Entry e = (Map.Entry) o;
                Object k = e.getKey();
                Map v = (Map) e.getValue();
                long executed = getByKeywordOr0(v, EXECUTED).longValue();
                if (executed > 0) {
                    double executeLatencyTotal = getByKeywordOr0(v, EXEC_LAT_TOTAL).doubleValue();
                    double processLatencyTotal = getByKeywordOr0(v, PROC_LAT_TOTAL).doubleValue();
                    putRawKV(v, EXEC_LATENCY, executeLatencyTotal / executed);
                    putRawKV(v, PROC_LATENCY, processLatencyTotal / executed);
                } else {
                    putRawKV(v, EXEC_LATENCY, 0.0);
                    putRawKV(v, PROC_LATENCY, 0.0);
                }
                removeByKeyword(v, EXEC_LAT_TOTAL);
                removeByKeyword(v, PROC_LAT_TOTAL);
                inStats2.put(k, v);
            }
            putRawKV(ret, CID_SID_TO_IN_STATS, inStats2);

            putRawKV(ret, SID_TO_OUT_STATS, outStats);
            putRawKV(ret, WIN_TO_EXECUTED, mapKeyStr(getMapByKeyword(accData, WIN_TO_EXECUTED)));
            putRawKV(ret, WIN_TO_EXEC_LAT, computeWeightedAveragesPerWindow(
                    accData, WIN_TO_EXEC_LAT_WGT_AVG, WIN_TO_EXECUTED));
            putRawKV(ret, WIN_TO_PROC_LAT, computeWeightedAveragesPerWindow(
                    accData, WIN_TO_PROC_LAT_WGT_AVG, WIN_TO_EXECUTED));
        } else {
            Map outStats2 = new HashMap();
            for (Object o : outStats.entrySet()) {
                Map.Entry e = (Map.Entry) o;
                Object k = e.getKey();
                Map v = (Map) e.getValue();
                long acked = getByKeywordOr0(v, ACKED).longValue();
                if (acked > 0) {
                    double compLatencyTotal = getByKeywordOr0(v, COMP_LAT_TOTAL).doubleValue();
                    putRawKV(v, COMP_LATENCY, compLatencyTotal / acked);
                } else {
                    putRawKV(v, COMP_LATENCY, 0.0);
                }
                removeByKeyword(v, COMP_LAT_TOTAL);
                outStats2.put(k, v);
            }
            putRawKV(ret, SID_TO_OUT_STATS, outStats2);
            putRawKV(ret, WIN_TO_COMP_LAT, computeWeightedAveragesPerWindow(
                    accData, WIN_TO_COMP_LAT_WGT_AVG, WIN_TO_ACKED));
        }

        return ret;
    }

    public static ComponentPageInfo aggCompExecsStats(
            Map exec2hostPort, Map task2component, Map beats, String window, boolean includeSys,
            String topologyId, StormTopology topology, String componentId) {

        List beatList = extractDataFromHb(exec2hostPort, task2component, beats, includeSys, topology, componentId);
        Map compStats = aggregateCompStats(window, includeSys, beatList, componentType(topology, componentId).getName());
        compStats = postAggregateCompStats(task2component, exec2hostPort, compStats);
        return thriftifyCompPageData(topologyId, topology, componentId, compStats);
    }


    // =====================================================================================
    // clojurify stats methods
    // =====================================================================================

    public static Map clojurifyStats(Map stats) {
        Map ret = new HashMap();
        for (Object o : stats.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            ExecutorInfo executorInfo = (ExecutorInfo) entry.getKey();
            ExecutorStats executorStats = (ExecutorStats) entry.getValue();

            ret.put(Lists.newArrayList(executorInfo.get_task_start(), executorInfo.get_task_end()),
                    clojurifyExecutorStats(executorStats));
        }
        return ret;
    }

    public static Map clojurifyExecutorStats(ExecutorStats stats) {
        Map ret = new HashMap();

        putRawKV(ret, EMITTED, stats.get_emitted());
        putRawKV(ret, TRANSFERRED, stats.get_transferred());
        putRawKV(ret, "rate", stats.get_rate());

        if (stats.get_specific().is_set_bolt()) {
            mergeMaps(ret, clojurifySpecificStats(stats.get_specific().get_bolt()));
            putRawKV(ret, TYPE, KW_BOLT);
        } else {
            mergeMaps(ret, clojurifySpecificStats(stats.get_specific().get_spout()));
            putRawKV(ret, TYPE, KW_SPOUT);
        }

        return ret;
    }

    public static Map clojurifySpecificStats(SpoutStats stats) {
        Map ret = new HashMap();
        putRawKV(ret, ACKED, stats.get_acked());
        putRawKV(ret, FAILED, stats.get_failed());
        putRawKV(ret, COMP_LATENCIES, stats.get_complete_ms_avg());

        return ret;
    }

    public static Map clojurifySpecificStats(BoltStats stats) {
        Map ret = new HashMap();

        Map acked = windowSetConverter(stats.get_acked(), FROM_GSID, IDENTITY);
        Map failed = windowSetConverter(stats.get_failed(), FROM_GSID, IDENTITY);
        Map processAvg = windowSetConverter(stats.get_process_ms_avg(), FROM_GSID, IDENTITY);
        Map executed = windowSetConverter(stats.get_executed(), FROM_GSID, IDENTITY);
        Map executeAvg = windowSetConverter(stats.get_execute_ms_avg(), FROM_GSID, IDENTITY);

        putRawKV(ret, ACKED, acked);
        putRawKV(ret, FAILED, failed);
        putRawKV(ret, PROC_LATENCIES, processAvg);
        putRawKV(ret, EXECUTED, executed);
        putRawKV(ret, EXEC_LATENCIES, executeAvg);

        return ret;
    }

    public static List extractNodeInfosFromHbForComp(
            Map exec2hostPort, Map task2component, boolean includeSys, String compId) {
        List ret = new ArrayList();

        Set<List> hostPorts = new HashSet<>();
        for (Object o : exec2hostPort.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            List key = (List) entry.getKey();
            List value = (List) entry.getValue();

            Integer start = ((Number) key.get(0)).intValue();
            String host = (String) value.get(0);
            Integer port = (Integer) value.get(1);
            String comp = (String) task2component.get(start);
            if ((compId == null || compId.equals(comp)) && (includeSys || !Utils.isSystemId(comp))) {
                hostPorts.add(Lists.newArrayList(host, port));
            }
        }

        for (List hostPort : hostPorts) {
            Map m = new HashMap();
            putRawKV(m, HOST, hostPort.get(0));
            putRawKV(m, PORT, hostPort.get(1));
            ret.add(m);
        }

        return ret;
    }

    public static List extractDataFromHb(Map executor2hostPort, Map task2component, Map beats,
                                         boolean includeSys, StormTopology topology) {
        return extractDataFromHb(executor2hostPort, task2component, beats, includeSys, topology, null);
    }

    public static List extractDataFromHb(Map executor2hostPort, Map task2component, Map beats,
                                         boolean includeSys, StormTopology topology, String compId) {
        List ret = new ArrayList();
        if (executor2hostPort == null) {
            return ret;
        }
        for (Object o : executor2hostPort.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            List key = (List) entry.getKey();
            List value = (List) entry.getValue();

            Integer start = ((Number) key.get(0)).intValue();
            Integer end = ((Number) key.get(1)).intValue();

            String host = (String) value.get(0);
            Integer port = ((Number) value.get(1)).intValue();

            Map beat = (Map) beats.get(key);
            if (beat == null) {
                continue;
            }
            String id = (String) task2component.get(start);

            Map m = new HashMap();
            if ((compId == null || compId.equals(id)) && (includeSys || !Utils.isSystemId(id))) {
                putRawKV(m, "exec-id", entry.getKey());
                putRawKV(m, "comp-id", id);
                putRawKV(m, NUM_TASKS, end - start + 1);
                putRawKV(m, HOST, host);
                putRawKV(m, PORT, port);
                putRawKV(m, UPTIME, beat.get(keyword(UPTIME)));
                putRawKV(m, STATS, beat.get(keyword(STATS)));

                Keyword type = componentType(topology, compId);
                if (type != null) {
                    putRawKV(m, TYPE, type);
                } else {
                    putRawKV(m, TYPE, getByKeyword(getMapByKeyword(beat, STATS), TYPE));
                }
                ret.add(m);
            }
        }
        return ret;
    }

    private static Map computeWeightedAveragesPerWindow(Map accData, String wgtAvgKey, String divisorKey) {
        Map ret = new HashMap();
        for (Object o : getMapByKeyword(accData, wgtAvgKey).entrySet()) {
            Map.Entry e = (Map.Entry) o;
            Object window = e.getKey();
            double wgtAvg = ((Number) e.getValue()).doubleValue();
            long divisor = ((Number) getMapByKeyword(accData, divisorKey).get(window)).longValue();
            if (divisor > 0) {
                ret.put(window.toString(), wgtAvg / divisor);
            }
        }
        return ret;
    }


    /**
     * computes max bolt capacity
     *
     * @param executorSumms a list of ExecutorSummary
     * @return max bolt capacity
     */
    public static double computeBoltCapacity(List executorSumms) {
        double max = 0.0;
        for (Object o : executorSumms) {
            ExecutorSummary summary = (ExecutorSummary) o;
            double capacity = computeExecutorCapacity(summary);
            if (capacity > max) {
                max = capacity;
            }
        }
        return max;
    }

    public static double computeExecutorCapacity(ExecutorSummary summ) {
        ExecutorStats stats = summ.get_stats();
        if (stats == null) {
            return 0.0;
        } else {
            Map m = aggregateBoltStats(Lists.newArrayList(stats), true);
            m = swapMapOrder(aggregateBoltStreams(m));
            Map data = getMapByKeyword(m, TEN_MIN_IN_SECONDS_STR);

            int uptime = summ.get_uptime_secs();
            int win = Math.min(uptime, TEN_MIN_IN_SECONDS);
            long executed = getByKeywordOr0(data, EXECUTED).longValue();
            double latency = getByKeywordOr0(data, EXEC_LATENCIES).doubleValue();
            if (win > 0) {
                return executed * latency / (1000 * win);
            }
            return 0.0;
        }
    }

    /**
     * filter ExecutorSummary whose stats is null
     *
     * @param summs a list of ExecutorSummary
     * @return filtered summs
     */
    public static List getFilledStats(List summs) {
        for (Iterator itr = summs.iterator(); itr.hasNext(); ) {
            ExecutorSummary summ = (ExecutorSummary) itr.next();
            if (summ.get_stats() == null) {
                itr.remove();
            }
        }
        return summs;
    }

    private static Map mapKeyStr(Map m) {
        Map ret = new HashMap();
        for (Object k : m.keySet()) {
            ret.put(k.toString(), m.get(k));
        }
        return ret;
    }

    private static long sumStreamsLong(Map m, String key) {
        long sum = 0;
        if (m == null) {
            return sum;
        }
        for (Object v : m.values()) {
            Map sub = (Map) v;
            for (Object o : sub.entrySet()) {
                Map.Entry e = (Map.Entry) o;
                if (((Keyword) e.getKey()).getName().equals(key)) {
                    sum += ((Number) e.getValue()).longValue();
                }
            }
        }
        return sum;
    }

    private static double sumStreamsDouble(Map m, String key) {
        double sum = 0;
        if (m == null) {
            return sum;
        }
        for (Object v : m.values()) {
            Map sub = (Map) v;
            for (Object o : sub.entrySet()) {
                Map.Entry e = (Map.Entry) o;
                if (((Keyword) e.getKey()).getName().equals(key)) {
                    sum += ((Number) e.getValue()).doubleValue();
                }
            }
        }
        return sum;
    }

    /**
     * same as clojure's (merge-with merge m1 m2)
     */
    private static Map mergeMaps(Map m1, Map m2) {
        if (m2 == null) {
            return m1;
        }
        for (Object o : m2.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            Object k = entry.getKey();

            Map existing = (Map) m1.get(k);
            if (existing == null) {
                m1.put(k, entry.getValue());
            } else {
                existing.putAll((Map) m2.get(k));
            }
        }
        return m1;
    }

    /**
     * filter system streams from stats
     *
     * @param stats      { win -> stream id -> value }
     * @param includeSys whether to filter system streams
     * @return filtered stats
     */
    private static Map filterSysStreams(Map stats, boolean includeSys) {
        if (!includeSys) {
            for (Object win : stats.keySet()) {
                Map stream2stat = (Map) stats.get(win);
                for (Iterator itr = stream2stat.keySet().iterator(); itr.hasNext(); ) {
                    Object key = itr.next();
                    if (key instanceof String && Utils.isSystemId((String) key)) {
                        itr.remove();
                    }
                }
            }
        }
        return stats;
    }

    /**
     * equals to clojure's: (merge-with (partial merge-with sum-or-0) acc-out spout-out)
     */
    private static Map fullMergeWithSum(Map m1, Map m2) {
        Set<Object> allKeys = new HashSet<>();
        if (m1 != null) {
            allKeys.addAll(m1.keySet());
        }
        if (m2 != null) {
            allKeys.addAll(m2.keySet());
        }

        Map ret = new HashMap();
        for (Object k : allKeys) {
            Map mm1 = null, mm2 = null;
            if (m1 != null) {
                mm1 = (Map) m1.get(k);
            }
            if (m2 != null) {
                mm2 = (Map) m2.get(k);
            }
            ret.put(k, mergeWithSum(mm1, mm2));
        }

        return ret;
    }

    private static Map mergeWithSum(Map m1, Map m2) {
        Map ret = new HashMap();

        Set<Object> allKeys = new HashSet<>();
        if (m1 != null) {
            allKeys.addAll(m1.keySet());
        }
        if (m2 != null) {
            allKeys.addAll(m2.keySet());
        }

        for (Object k : allKeys) {
            Number n1 = getOr0(m1, k);
            Number n2 = getOr0(m2, k);
            ret.put(k, add(n1, n2));
        }
        return ret;
    }

    /**
     * this method merges 2 two-level-deep maps, which is different from mergeWithSum, and we expect the two maps
     * have the same keys
     */
    private static Map mergeWithAddPair(Map m1, Map m2) {
        Map ret = new HashMap();

        Set<Object> allKeys = new HashSet<>();
        if (m1 != null) {
            allKeys.addAll(m1.keySet());
        }
        if (m2 != null) {
            allKeys.addAll(m2.keySet());
        }

        for (Object k : allKeys) {
            Map mm1 = (m1 != null) ? (Map) m1.get(k) : null;
            Map mm2 = (m2 != null) ? (Map) m2.get(k) : null;
            if (mm1 == null && mm2 == null) {
                continue;
            } else if (mm1 == null) {
                ret.put(k, mm2);
            } else if (mm2 == null) {
                ret.put(k, mm1);
            } else {
                Map tmp = new HashMap();
                for (Object kk : mm1.keySet()) {
                    List seq1 = (List) mm1.get(kk);
                    List seq2 = (List) mm2.get(kk);
                    List sums = new ArrayList();
                    for (int i = 0; i < seq1.size(); i++) {
                        sums.add(add((Number) seq1.get(i), (Number) seq2.get(i)));
                    }
                    tmp.put(kk, sums);
                }
                ret.put(k, tmp);
            }
        }
        return ret;
    }

    // =====================================================================================
    // thriftify stats methods
    // =====================================================================================

    private static TopologyPageInfo thriftifyTopoPageData(String topologyId, Map data) {
        TopologyPageInfo ret = new TopologyPageInfo(topologyId);
        Integer numTasks = getByKeywordOr0(data, NUM_TASKS).intValue();
        Integer numWorkers = getByKeywordOr0(data, NUM_WORKERS).intValue();
        Integer numExecutors = getByKeywordOr0(data, NUM_EXECUTORS).intValue();
        Map spout2stats = getMapByKeyword(data, SPOUT_TO_STATS);
        Map bolt2stats = getMapByKeyword(data, BOLT_TO_STATS);
        Map win2emitted = getMapByKeyword(data, WIN_TO_EMITTED);
        Map win2transferred = getMapByKeyword(data, WIN_TO_TRANSFERRED);
        Map win2compLatency = getMapByKeyword(data, WIN_TO_COMP_LAT);
        Map win2acked = getMapByKeyword(data, WIN_TO_ACKED);
        Map win2failed = getMapByKeyword(data, WIN_TO_FAILED);

        Map<String, ComponentAggregateStats> spoutAggStats = new HashMap<>();
        for (Object o : spout2stats.entrySet()) {
            Map.Entry e = (Map.Entry) o;
            String id = (String) e.getKey();
            Map v = (Map) e.getValue();
            putRawKV(v, TYPE, KW_SPOUT);

            spoutAggStats.put(id, thriftifySpoutAggStats(v));
        }

        Map<String, ComponentAggregateStats> boltAggStats = new HashMap<>();
        for (Object o : bolt2stats.entrySet()) {
            Map.Entry e = (Map.Entry) o;
            String id = (String) e.getKey();
            Map v = (Map) e.getValue();
            putRawKV(v, TYPE, KW_BOLT);

            boltAggStats.put(id, thriftifyBoltAggStats(v));
        }

        TopologyStats topologyStats = new TopologyStats();
        topologyStats.set_window_to_acked(win2acked);
        topologyStats.set_window_to_emitted(win2emitted);
        topologyStats.set_window_to_failed(win2failed);
        topologyStats.set_window_to_transferred(win2transferred);
        topologyStats.set_window_to_complete_latencies_ms(win2compLatency);

        ret.set_num_tasks(numTasks);
        ret.set_num_workers(numWorkers);
        ret.set_num_executors(numExecutors);
        ret.set_id_to_spout_agg_stats(spoutAggStats);
        ret.set_id_to_bolt_agg_stats(boltAggStats);
        ret.set_topology_stats(topologyStats);

        return ret;
    }

    private static ComponentAggregateStats thriftifySpoutAggStats(Map m) {
        logger.warn("spout agg stats:{}", m);
        ComponentAggregateStats stats = new ComponentAggregateStats();
        stats.set_type(ComponentType.SPOUT);
        stats.set_last_error((ErrorInfo) getByKeyword(m, LAST_ERROR));
        thriftifyCommonAggStats(stats, m);

        SpoutAggregateStats spoutAggStats = new SpoutAggregateStats();
        spoutAggStats.set_complete_latency_ms(getByKeywordOr0(m, COMP_LATENCY).doubleValue());
        SpecificAggregateStats specificStats = SpecificAggregateStats.spout(spoutAggStats);

        stats.set_specific_stats(specificStats);
        return stats;
    }

    private static ComponentAggregateStats thriftifyBoltAggStats(Map m) {
        ComponentAggregateStats stats = new ComponentAggregateStats();
        stats.set_type(ComponentType.BOLT);
        stats.set_last_error((ErrorInfo) getByKeyword(m, LAST_ERROR));
        thriftifyCommonAggStats(stats, m);

        BoltAggregateStats boltAggStats = new BoltAggregateStats();
        boltAggStats.set_execute_latency_ms(getByKeywordOr0(m, EXEC_LATENCY).doubleValue());
        boltAggStats.set_process_latency_ms(getByKeywordOr0(m, PROC_LATENCY).doubleValue());
        boltAggStats.set_executed(getByKeywordOr0(m, EXECUTED).longValue());
        boltAggStats.set_capacity(getByKeywordOr0(m, CAPACITY).doubleValue());
        SpecificAggregateStats specificStats = SpecificAggregateStats.bolt(boltAggStats);

        stats.set_specific_stats(specificStats);
        return stats;
    }

    private static ExecutorAggregateStats thriftifyExecAggStats(String compId, Keyword compType, Map m) {
        ExecutorAggregateStats stats = new ExecutorAggregateStats();

        ExecutorSummary executorSummary = new ExecutorSummary();
        List executor = (List) getByKeyword(m, EXECUTOR_ID);
        executorSummary.set_executor_info(new ExecutorInfo(((Number) executor.get(0)).intValue(),
                ((Number) executor.get(1)).intValue()));
        executorSummary.set_component_id(compId);
        executorSummary.set_host((String) getByKeyword(m, HOST));
        executorSummary.set_port(getByKeywordOr0(m, PORT).intValue());
        int uptime = getByKeywordOr0(m, UPTIME).intValue();
        executorSummary.set_uptime_secs(uptime);
        stats.set_exec_summary(executorSummary);

        if (compType.getName().equals(SPOUT)) {
            stats.set_stats(thriftifySpoutAggStats(m));
        } else {
            stats.set_stats(thriftifyBoltAggStats(m));
        }

        return stats;
    }

    private static Map thriftifyBoltOutputStats(Map id2outStats) {
        Map ret = new HashMap();
        for (Object k : id2outStats.keySet()) {
            ret.put(k, thriftifyBoltAggStats((Map) id2outStats.get(k)));
        }
        return ret;
    }

    private static Map thriftifySpoutOutputStats(Map id2outStats) {
        Map ret = new HashMap();
        for (Object k : id2outStats.keySet()) {
            ret.put(k, thriftifySpoutAggStats((Map) id2outStats.get(k)));
        }
        return ret;
    }

    private static Map thriftifyBoltInputStats(Map cidSid2inputStats) {
        Map ret = new HashMap();
        for (Object e : cidSid2inputStats.entrySet()) {
            Map.Entry entry = (Map.Entry) e;
            ret.put(toGlobalStreamId((List) entry.getKey()),
                    thriftifyBoltAggStats((Map) entry.getValue()));
        }
        return ret;
    }

    private static ComponentAggregateStats thriftifyCommonAggStats(ComponentAggregateStats stats, Map m) {
        CommonAggregateStats commonStats = new CommonAggregateStats();
        commonStats.set_num_tasks(getByKeywordOr0(m, NUM_TASKS).intValue());
        commonStats.set_num_executors(getByKeywordOr0(m, NUM_EXECUTORS).intValue());
        commonStats.set_emitted(getByKeywordOr0(m, EMITTED).longValue());
        commonStats.set_transferred(getByKeywordOr0(m, TRANSFERRED).longValue());
        commonStats.set_acked(getByKeywordOr0(m, ACKED).longValue());
        commonStats.set_failed(getByKeywordOr0(m, FAILED).longValue());

        stats.set_common_stats(commonStats);
        return stats;
    }

    private static ComponentPageInfo thriftifyCompPageData(
            String topologyId, StormTopology topology, String compId, Map data) {
        ComponentPageInfo ret = new ComponentPageInfo();
        ret.set_component_id(compId);

        Map win2stats = new HashMap();
        putRawKV(win2stats, EMITTED, getMapByKeyword(data, WIN_TO_EMITTED));
        putRawKV(win2stats, TRANSFERRED, getMapByKeyword(data, WIN_TO_TRANSFERRED));
        putRawKV(win2stats, ACKED, getMapByKeyword(data, WIN_TO_ACKED));
        putRawKV(win2stats, FAILED, getMapByKeyword(data, WIN_TO_FAILED));

        Keyword type = (Keyword) getByKeyword(data, TYPE);
        String compType = type.getName();
        if (compType.equals(SPOUT)) {
            ret.set_component_type(ComponentType.SPOUT);
            putRawKV(win2stats, COMP_LATENCY, getMapByKeyword(data, WIN_TO_COMP_LAT));
        } else {
            ret.set_component_type(ComponentType.BOLT);
            putRawKV(win2stats, EXEC_LATENCY, getMapByKeyword(data, WIN_TO_EXEC_LAT));
            putRawKV(win2stats, PROC_LATENCY, getMapByKeyword(data, WIN_TO_PROC_LAT));
            putRawKV(win2stats, EXECUTED, getMapByKeyword(data, WIN_TO_EXECUTED));
        }
        win2stats = swapMapOrder(win2stats);

        List<ExecutorAggregateStats> execStats = new ArrayList<>();
        List executorStats = (List) getByKeyword(data, EXECUTOR_STATS);
        if (executorStats != null) {
            for (Object o : executorStats) {
                execStats.add(thriftifyExecAggStats(compId, type, (Map) o));
            }
        }

        Map gsid2inputStats, sid2outputStats;
        if (compType.equals(SPOUT)) {
            Map tmp = new HashMap();
            for (Object k : win2stats.keySet()) {
                tmp.put(k, thriftifySpoutAggStats((Map) win2stats.get(k)));
            }
            win2stats = tmp;
            gsid2inputStats = null;
            sid2outputStats = thriftifySpoutOutputStats(getMapByKeyword(data, SID_TO_OUT_STATS));
        } else {
            Map tmp = new HashMap();
            for (Object k : win2stats.keySet()) {
                tmp.put(k, thriftifyBoltAggStats((Map) win2stats.get(k)));
            }
            win2stats = tmp;
            gsid2inputStats = thriftifyBoltInputStats(getMapByKeyword(data, CID_SID_TO_IN_STATS));
            sid2outputStats = thriftifyBoltOutputStats(getMapByKeyword(data, SID_TO_OUT_STATS));
        }
        ret.set_num_executors(getByKeywordOr0(data, NUM_EXECUTORS).intValue());
        ret.set_num_tasks(getByKeywordOr0(data, NUM_TASKS).intValue());
        ret.set_topology_id(topologyId);
        ret.set_topology_name(null);
        ret.set_window_to_stats(win2stats);
        ret.set_sid_to_output_stats(sid2outputStats);
        ret.set_exec_stats(execStats);
        ret.set_gsid_to_input_stats(gsid2inputStats);

        return ret;
    }

    public static Map thriftifyStats(List stats) {
        Map ret = new HashMap();
        for (Object o : stats) {
            List stat = (List) o;
            List executor = (List) stat.get(0);
            int start = ((Number) executor.get(0)).intValue();
            int end = ((Number) executor.get(1)).intValue();
            Map executorStat = (Map) stat.get(1);
            ExecutorInfo executorInfo = new ExecutorInfo(start, end);
            ret.put(executorInfo, thriftifyExecutorStats(executorStat));
        }
        return ret;
    }

    public static ExecutorStats thriftifyExecutorStats(Map stats) {
        ExecutorStats ret = new ExecutorStats();
        ExecutorSpecificStats specificStats = thriftifySpecificStats(stats);
        ret.set_specific(specificStats);

        ret.set_emitted(windowSetConverter(getMapByKeyword(stats, EMITTED), TO_STRING, TO_STRING));
        ret.set_transferred(windowSetConverter(getMapByKeyword(stats, TRANSFERRED), TO_STRING, TO_STRING));
        ret.set_rate(((Number) getByKeyword(stats, "rate")).doubleValue());

        return ret;
    }

    private static ExecutorSpecificStats thriftifySpecificStats(Map stats) {
        ExecutorSpecificStats specificStats = new ExecutorSpecificStats();

        String compType = ((Keyword) getByKeyword(stats, TYPE)).getName();
        if (BOLT.equals(compType)) {
            BoltStats boltStats = new BoltStats();
            boltStats.set_acked(windowSetConverter(getMapByKeyword(stats, ACKED), TO_GSID, TO_STRING));
            boltStats.set_executed(windowSetConverter(getMapByKeyword(stats, EXECUTED), TO_GSID, TO_STRING));
            boltStats.set_execute_ms_avg(windowSetConverter(getMapByKeyword(stats, EXEC_LATENCIES), TO_GSID, TO_STRING));
            boltStats.set_failed(windowSetConverter(getMapByKeyword(stats, FAILED), TO_GSID, TO_STRING));
            boltStats.set_process_ms_avg(windowSetConverter(getMapByKeyword(stats, PROC_LATENCIES), TO_GSID, TO_STRING));
            specificStats.set_bolt(boltStats);
        } else {
            SpoutStats spoutStats = new SpoutStats();
            spoutStats.set_acked(windowSetConverter(getMapByKeyword(stats, ACKED), TO_STRING, TO_STRING));
            spoutStats.set_failed(windowSetConverter(getMapByKeyword(stats, FAILED), TO_STRING, TO_STRING));
            spoutStats.set_complete_ms_avg(windowSetConverter(getMapByKeyword(stats, COMP_LATENCIES), TO_STRING, TO_STRING));
            specificStats.set_spout(spoutStats);
        }
        return specificStats;
    }


    // =====================================================================================
    // helper methods
    // =====================================================================================

    private static GlobalStreamId toGlobalStreamId(List list) {
        return new GlobalStreamId((String) list.get(0), (String) list.get(1));
    }

    /**
     * Returns true if x is a number that is not NaN or Infinity, false otherwise
     */
    private static boolean isValidNumber(Object x) {
        return x != null && x instanceof Number &&
                !Double.isNaN(((Number) x).doubleValue()) &&
                !Double.isInfinite(((Number) x).doubleValue());
    }

    /**
     * the value of m is as follows:
     * <pre>
     * #org.apache.storm.stats.CommonStats {
     *  :executed {
     *      ":all-time" {["split" "default"] 18727460},
     *      "600" {["split" "default"] 11554},
     *      "10800" {["split" "default"] 207269},
     *      "86400" {["split" "default"] 1659614}},
     *  :execute-latencies {
     *      ":all-time" {["split" "default"] 0.5874528633354443},
     *      "600" {["split" "default"] 0.6140350877192983},
     *      "10800" {["split" "default"] 0.5864434687156971},
     *      "86400" {["split" "default"] 0.5815376460556336}}
     * }
     * </pre>
     */
    private static double computeAggCapacity(Map m, Integer uptime) {
        if (uptime != null) {
            Map execAvg = (Map) ((Map) getByKeyword(m, EXEC_LATENCIES)).get(TEN_MIN_IN_SECONDS_STR);
            Map exec = (Map) ((Map) getByKeyword(m, EXECUTED)).get(TEN_MIN_IN_SECONDS_STR);

            Set<Object> allKeys = new HashSet<>();
            if (execAvg != null) {
                allKeys.addAll(execAvg.keySet());
            }
            if (exec != null) {
                allKeys.addAll(exec.keySet());
            }

            double totalAvg = 0;
            for (Object k : allKeys) {
                double avg = getOr0(execAvg, k).doubleValue();
                long cnt = getOr0(exec, k).longValue();
                totalAvg += avg * cnt;
            }
            return totalAvg / (Math.min(uptime, TEN_MIN_IN_SECONDS) * 1000);
        }
        return 0.0;
    }

    private static Number getOr0(Map m, Object k) {
        if (m == null) {
            return 0;
        }

        Number n = (Number) m.get(k);
        if (n == null) {
            return 0;
        }
        return n;
    }

    private static Number getByKeywordOr0(Map m, String k) {
        if (m == null) {
            return 0;
        }

        Number n = (Number) m.get(keyword(k));
        if (n == null) {
            return 0;
        }
        return n;
    }

    private static Double weightAvgAndSum(Map id2Avg, Map id2num) {
        double ret = 0;
        if (id2Avg == null || id2num == null) {
            return ret;
        }

        for (Object o : id2Avg.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            Object k = entry.getKey();
            double v = ((Number) entry.getValue()).doubleValue();
            long n = ((Number) id2num.get(k)).longValue();
            ret += productOr0(v, n);
        }
        return ret;
    }

    private static double weightAvg(Map id2Avg, Map id2num, Object key) {
        if (id2Avg == null || id2num == null) {
            return 0.0;
        }
        return productOr0(id2Avg.get(key), id2num.get(key));
    }

    public static Keyword componentType(StormTopology topology, String compId) {
        if (compId == null) {
            return null;
        }

        Map<String, Bolt> bolts = topology.get_bolts();
        if (Utils.isSystemId(compId) || bolts.containsKey(compId)) {
            return KW_BOLT;
        }
        return KW_SPOUT;
    }

    public static void putRawKV(Map map, String k, Object v) {
        map.put(keyword(k), v);
    }

    private static void removeByKeyword(Map map, String k) {
        map.remove(keyword(k));
    }

    public static Object getByKeyword(Map map, String key) {
        return map.get(keyword(key));
    }

    public static Map getMapByKeyword(Map map, String key) {
        if (map == null) {
            return null;
        }
        return (Map) map.get(keyword(key));
    }

    private static Number add(Number n1, Number n2) {
        if (n1 instanceof Long || n1 instanceof Integer) {
            return n1.longValue() + n2.longValue();
        }
        return n1.doubleValue() + n2.doubleValue();
    }

    private static long sumValues(Map m) {
        long ret = 0L;
        if (m == null) {
            return ret;
        }

        for (Object o : m.values()) {
            ret += ((Number) o).longValue();
        }
        return ret;
    }

    private static Number sumOr0(Object a, Object b) {
        if (isValidNumber(a) && isValidNumber(b)) {
            if (a instanceof Long || a instanceof Integer) {
                return ((Number) a).longValue() + ((Number) b).longValue();
            } else {
                return ((Number) a).doubleValue() + ((Number) b).doubleValue();
            }
        }
        return 0;
    }

    private static double productOr0(Object a, Object b) {
        if (isValidNumber(a) && isValidNumber(b)) {
            return ((Number) a).doubleValue() * ((Number) b).doubleValue();
        }
        return 0;
    }

    private static double maxOr0(Object a, Object b) {
        if (isValidNumber(a) && isValidNumber(b)) {
            return Math.max(((Number) a).doubleValue(), ((Number) b).doubleValue());
        }
        return 0;
    }

    /**
     * For a nested map, rearrange data such that the top-level keys become the
     * nested map's keys and vice versa.
     * Example:
     * {:a {:X :banana, :Y :pear}, :b {:X :apple, :Y :orange}}
     * -> {:Y {:a :pear, :b :orange}, :X {:a :banana, :b :apple}}"
     */
    private static Map swapMapOrder(Map m) {
        if (m.size() == 0) {
            return m;
        }

        Map ret = new HashMap();
        for (Object k1 : m.keySet()) {
            Map v = (Map) m.get(k1);
            if (v != null) {
                for (Object k2 : v.keySet()) {
                    Map subRet = (Map) ret.get(k2);
                    if (subRet == null) {
                        subRet = new HashMap();
                        ret.put(k2, subRet);
                    }
                    subRet.put(k1, v.get(k2));
                }
            }
        }
        return ret;
    }

    /**
     * @param avgs   a PersistentHashMap of values: { win -> GlobalStreamId -> value }
     * @param counts a PersistentHashMap of values: { win -> GlobalStreamId -> value }
     * @return a PersistentHashMap of values: {win -> GlobalStreamId -> [cnt*avg, cnt]}
     */
    private static Map expandAverages(Map avgs, Map counts) {
        Map ret = new HashMap();

        for (Object win : counts.keySet()) {
            Map inner = new HashMap();

            Map stream2cnt = (Map) counts.get(win);
            for (Object stream : stream2cnt.keySet()) {
                Long cnt = (Long) stream2cnt.get(stream);
                Double avg = (Double) ((Map) avgs.get(win)).get(stream);
                if (avg == null) {
                    avg = 0.0;
                }
                inner.put(stream, Lists.newArrayList(cnt * avg, cnt));
            }
            ret.put(win, inner);
        }

        return ret;
    }

    /**
     * first zip the two seqs, then do expand-average, then merge with sum
     *
     * @param avgSeq   list of avgs like: [{win -> GlobalStreamId -> value}, ...]
     * @param countSeq list of counts like [{win -> GlobalStreamId -> value}, ...]
     */
    private static Map expandAveragesSeq(List avgSeq, List countSeq) {
        Map initVal = null;
        for (int i = 0; i < avgSeq.size(); i++) {
            Map avg = (Map) avgSeq.get(i);
            Map count = (Map) countSeq.get(i);
            if (initVal == null) {
                initVal = expandAverages(avg, count);
            } else {
                initVal = mergeWithAddPair(initVal, expandAverages(avg, count));
            }
        }
        return initVal;
    }

    private static double valAvg(double t, long c) {
        if (c == 0) {
            return 0;
        }
        return t / c;
    }

    public static String floatStr(Double n) {
        if (n == null) {
            return "0";
        }
        return String.format("%.3f", n);
    }

    public static String errorSubset(String errorStr) {
        return errorStr.substring(0, 200);
    }

    private static Keyword keyword(String key) {
        return RT.keyword(null, key);
    }

    private static ErrorInfo getLastError(IStormClusterState stormClusterState, String stormId, String compId) {
        return stormClusterState.lastError(stormId, compId);
    }

    interface KeyTransformer<T> {
        T transform(Object key);
    }

    static class ToGlobalStreamIdTransformer implements KeyTransformer<GlobalStreamId> {
        @Override
        public GlobalStreamId transform(Object key) {
            if (key instanceof List) {
                List l = (List) key;
                if (l.size() > 1) {
                    return new GlobalStreamId((String) l.get(0), (String) l.get(1));
                }
            }
            return new GlobalStreamId("", key.toString());
        }
    }

    static class FromGlobalStreamIdTransformer implements KeyTransformer<List> {
        @Override
        public List transform(Object key) {
            GlobalStreamId sid = (GlobalStreamId) key;
            return Lists.newArrayList(sid.get_componentId(), sid.get_streamId());
        }
    }

    static class IdentityTransformer implements KeyTransformer<Object> {
        @Override
        public Object transform(Object key) {
            return key;
        }
    }

    static class ToStringTransformer implements KeyTransformer<String> {
        @Override
        public String transform(Object key) {
            return key.toString();
        }
    }

    public static <K> Map windowSetConverter(Map stats, KeyTransformer<K> firstKeyFunc) {
        return windowSetConverter(stats, IDENTITY, firstKeyFunc);
    }

    public static <K1, K2> Map windowSetConverter(
            Map stats, KeyTransformer<K2> secKeyFunc, KeyTransformer<K1> firstKeyFunc) {
        Map ret = new HashMap();

        for (Object o : stats.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            K1 key1 = firstKeyFunc.transform(entry.getKey());

            Map subRetMap = (Map) ret.get(key1);
            if (subRetMap == null) {
                subRetMap = new HashMap();
            }
            ret.put(key1, subRetMap);

            Map value = (Map) entry.getValue();
            for (Object oo : value.entrySet()) {
                Map.Entry subEntry = (Map.Entry) oo;
                K2 key2 = secKeyFunc.transform(subEntry.getKey());
                subRetMap.put(key2, subEntry.getValue());
            }
        }
        return ret;
    }
}
