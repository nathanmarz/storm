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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.storm.cluster.ExecutorBeat;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.BoltAggregateStats;
import org.apache.storm.generated.BoltStats;
import org.apache.storm.generated.ClusterWorkerHeartbeat;
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
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unchecked")
public class StatsUtil {
    private static final Logger logger = LoggerFactory.getLogger(StatsUtil.class);

    public static final String TYPE = "type";
    public static final String SPOUT = "spout";
    public static final String BOLT = "bolt";

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
    private static final String HEARTBEAT = "heartbeat";
    private static final String TIME_SECS = "time-secs";

    private static final String RATE = "rate";
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

    public static final IdentityTransformer IDENTITY = new IdentityTransformer();
    private static final ToStringTransformer TO_STRING = new ToStringTransformer();
    private static final FromGlobalStreamIdTransformer FROM_GSID = new FromGlobalStreamIdTransformer();
    public static final ToGlobalStreamIdTransformer TO_GSID = new ToGlobalStreamIdTransformer();


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
    public static Map<String, Number> aggBoltLatAndCount(Map<List<String>, Double> id2execAvg,
                                                         Map<List<String>, Double> id2procAvg,
                                                         Map<List<String>, Long> id2numExec) {
        Map<String, Number> ret = new HashMap<>();
        putKV(ret, EXEC_LAT_TOTAL, weightAvgAndSum(id2execAvg, id2numExec));
        putKV(ret, PROC_LAT_TOTAL, weightAvgAndSum(id2procAvg, id2numExec));
        putKV(ret, EXECUTED, sumValues(id2numExec));

        return ret;
    }

    /**
     * aggregate number acked and complete latencies across all streams.
     */
    public static Map<String, Number> aggSpoutLatAndCount(Map<String, Double> id2compAvg,
                                                          Map<String, Long> id2numAcked) {
        Map<String, Number> ret = new HashMap<>();
        putKV(ret, COMP_LAT_TOTAL, weightAvgAndSum(id2compAvg, id2numAcked));
        putKV(ret, ACKED, sumValues(id2numAcked));

        return ret;
    }

    /**
     * aggregate number executed and process & execute latencies.
     */
    public static <K> Map<K, Map> aggBoltStreamsLatAndCount(Map<K, Double> id2execAvg,
                                                            Map<K, Double> id2procAvg,
                                                            Map<K, Long> id2numExec) {
        Map<K, Map> ret = new HashMap<>();
        if (id2execAvg == null || id2procAvg == null || id2numExec == null) {
            return ret;
        }
        for (K k : id2execAvg.keySet()) {
            Map<String, Object> subMap = new HashMap<>();
            putKV(subMap, EXEC_LAT_TOTAL, weightAvg(id2execAvg, id2numExec, k));
            putKV(subMap, PROC_LAT_TOTAL, weightAvg(id2procAvg, id2numExec, k));
            putKV(subMap, EXECUTED, id2numExec.get(k));
            ret.put(k, subMap);
        }
        return ret;
    }

    /**
     * Aggregates number acked and complete latencies.
     */
    public static <K> Map<K, Map> aggSpoutStreamsLatAndCount(Map<K, Double> id2compAvg,
                                                             Map<K, Long> id2acked) {
        Map<K, Map> ret = new HashMap<>();
        if (id2compAvg == null || id2acked == null) {
            return ret;
        }
        for (K k : id2compAvg.keySet()) {
            Map subMap = new HashMap();
            putKV(subMap, COMP_LAT_TOTAL, weightAvg(id2compAvg, id2acked, k));
            putKV(subMap, ACKED, id2acked.get(k));
            ret.put(k, subMap);
        }
        return ret;
    }

    /**
     * pre-merge component page bolt stats from an executor heartbeat
     * 1. computes component capacity
     * 2. converts map keys of stats
     * 3. filters streams if necessary
     *
     * @param beat       executor heartbeat data
     * @param window     specified window
     * @param includeSys whether to include system streams
     * @return per-merged stats
     */
    public static Map<String, Object> aggPreMergeCompPageBolt(Map<String, Object> beat, String window, boolean includeSys) {
        Map<String, Object> ret = new HashMap<>();

        putKV(ret, EXECUTOR_ID, getByKey(beat, "exec-id"));
        putKV(ret, HOST, getByKey(beat, HOST));
        putKV(ret, PORT, getByKey(beat, PORT));
        putKV(ret, UPTIME, getByKey(beat, UPTIME));
        putKV(ret, NUM_EXECUTORS, 1);
        putKV(ret, NUM_TASKS, getByKey(beat, NUM_TASKS));

        Map stat2win2sid2num = getMapByKey(beat, STATS);
        putKV(ret, CAPACITY, computeAggCapacity(stat2win2sid2num, getByKeyOr0(beat, UPTIME).intValue()));

        // calc cid+sid->input_stats
        Map inputStats = new HashMap();
        Map sid2acked = (Map) windowSetConverter(getMapByKey(stat2win2sid2num, ACKED), TO_STRING).get(window);
        Map sid2failed = (Map) windowSetConverter(getMapByKey(stat2win2sid2num, FAILED), TO_STRING).get(window);
        putKV(inputStats, ACKED, sid2acked != null ? sid2acked : new HashMap());
        putKV(inputStats, FAILED, sid2failed != null ? sid2failed : new HashMap());

        inputStats = swapMapOrder(inputStats);

        Map sid2execLat = (Map) windowSetConverter(getMapByKey(stat2win2sid2num, EXEC_LATENCIES), TO_STRING).get(window);
        Map sid2procLat = (Map) windowSetConverter(getMapByKey(stat2win2sid2num, PROC_LATENCIES), TO_STRING).get(window);
        Map sid2exec = (Map) windowSetConverter(getMapByKey(stat2win2sid2num, EXECUTED), TO_STRING).get(window);
        mergeMaps(inputStats, aggBoltStreamsLatAndCount(sid2execLat, sid2procLat, sid2exec));
        putKV(ret, CID_SID_TO_IN_STATS, inputStats);

        // calc sid->output_stats
        Map outputStats = new HashMap();
        Map sid2emitted = (Map) windowSetConverter(getMapByKey(stat2win2sid2num, EMITTED), TO_STRING).get(window);
        Map sid2transferred = (Map) windowSetConverter(getMapByKey(stat2win2sid2num, TRANSFERRED), TO_STRING).get(window);
        if (sid2emitted != null) {
            putKV(outputStats, EMITTED, filterSysStreams(sid2emitted, includeSys));
        } else {
            putKV(outputStats, EMITTED, new HashMap());
        }
        if (sid2transferred != null) {
            putKV(outputStats, TRANSFERRED, filterSysStreams(sid2transferred, includeSys));
        } else {
            putKV(outputStats, TRANSFERRED, new HashMap());
        }
        outputStats = swapMapOrder(outputStats);
        putKV(ret, SID_TO_OUT_STATS, outputStats);

        return ret;
    }

    /**
     * pre-merge component page spout stats from an executor heartbeat
     * 1. computes component capacity
     * 2. converts map keys of stats
     * 3. filters streams if necessary
     *
     * @param beat       executor heartbeat data
     * @param window     specified window
     * @param includeSys whether to include system streams
     * @return per-merged stats
     */
    public static Map<String, Object> aggPreMergeCompPageSpout(Map<String, Object> beat, String window, boolean includeSys) {
        Map<String, Object> ret = new HashMap<>();
        putKV(ret, EXECUTOR_ID, getByKey(beat, "exec-id"));
        putKV(ret, HOST, getByKey(beat, HOST));
        putKV(ret, PORT, getByKey(beat, PORT));
        putKV(ret, UPTIME, getByKey(beat, UPTIME));
        putKV(ret, NUM_EXECUTORS, 1);
        putKV(ret, NUM_TASKS, getByKey(beat, NUM_TASKS));

        Map stat2win2sid2num = getMapByKey(beat, STATS);

        // calc sid->output-stats
        Map outputStats = new HashMap();
        Map win2sid2acked = windowSetConverter(getMapByKey(stat2win2sid2num, ACKED), TO_STRING);
        Map win2sid2failed = windowSetConverter(getMapByKey(stat2win2sid2num, FAILED), TO_STRING);
        Map win2sid2emitted = windowSetConverter(getMapByKey(stat2win2sid2num, EMITTED), TO_STRING);
        Map win2sid2transferred = windowSetConverter(getMapByKey(stat2win2sid2num, TRANSFERRED), TO_STRING);
        Map win2sid2compLat = windowSetConverter(getMapByKey(stat2win2sid2num, COMP_LATENCIES), TO_STRING);

        putKV(outputStats, ACKED, win2sid2acked.get(window));
        putKV(outputStats, FAILED, win2sid2failed.get(window));
        putKV(outputStats, EMITTED, filterSysStreams((Map) win2sid2emitted.get(window), includeSys));
        putKV(outputStats, TRANSFERRED, filterSysStreams((Map) win2sid2transferred.get(window), includeSys));
        outputStats = swapMapOrder(outputStats);

        Map sid2compLat = (Map) win2sid2compLat.get(window);
        Map sid2acked = (Map) win2sid2acked.get(window);
        mergeMaps(outputStats, aggSpoutStreamsLatAndCount(sid2compLat, sid2acked));
        putKV(ret, SID_TO_OUT_STATS, outputStats);

        return ret;
    }

    /**
     * pre-merge component stats of specified bolt id
     *
     * @param beat       executor heartbeat data
     * @param window     specified window
     * @param includeSys whether to include system streams
     * @return { comp id -> comp-stats }
     */
    public static <K, V extends Number> Map<String, Object> aggPreMergeTopoPageBolt(
            Map<String, Object> beat, String window, boolean includeSys) {
        Map<String, Object> ret = new HashMap<>();

        Map<String, Object> subRet = new HashMap<>();
        putKV(subRet, NUM_EXECUTORS, 1);
        putKV(subRet, NUM_TASKS, getByKey(beat, NUM_TASKS));

        Map<String, Object> stat2win2sid2num = getMapByKey(beat, STATS);
        putKV(subRet, CAPACITY, computeAggCapacity(stat2win2sid2num, getByKeyOr0(beat, UPTIME).intValue()));

        for (String key : new String[]{EMITTED, TRANSFERRED, ACKED, FAILED}) {
            Map<String, Map<K, V>> stat = windowSetConverter(getMapByKey(stat2win2sid2num, key), TO_STRING);
            if (EMITTED.equals(key) || TRANSFERRED.equals(key)) {
                stat = filterSysStreams(stat, includeSys);
            }
            Map<K, V> winStat = stat.get(window);
            long sum = 0;
            if (winStat != null) {
                for (V v : winStat.values()) {
                    sum += v.longValue();
                }
            }
            putKV(subRet, key, sum);
        }

        Map<String, Map<List<String>, Double>> win2sid2execLat =
                windowSetConverter(getMapByKey(stat2win2sid2num, EXEC_LATENCIES), TO_STRING);
        Map<String, Map<List<String>, Double>> win2sid2procLat =
                windowSetConverter(getMapByKey(stat2win2sid2num, PROC_LATENCIES), TO_STRING);
        Map<String, Map<List<String>, Long>> win2sid2exec =
                windowSetConverter(getMapByKey(stat2win2sid2num, EXECUTED), TO_STRING);
        subRet.putAll(aggBoltLatAndCount(
                win2sid2execLat.get(window), win2sid2procLat.get(window), win2sid2exec.get(window)));

        ret.put((String) getByKey(beat, "comp-id"), subRet);
        return ret;
    }

    /**
     * pre-merge component stats of specified spout id and returns { comp id -> comp-stats }
     */
    public static <K, V extends Number> Map<String, Object> aggPreMergeTopoPageSpout(
            Map<String, Object> m, String window, boolean includeSys) {
        Map<String, Object> ret = new HashMap<>();

        Map<String, Object> subRet = new HashMap<>();
        putKV(subRet, NUM_EXECUTORS, 1);
        putKV(subRet, NUM_TASKS, getByKey(m, NUM_TASKS));

        // no capacity for spout
        Map<String, Map<String, Map<String, V>>> stat2win2sid2num = getMapByKey(m, STATS);
        for (String key : new String[]{EMITTED, TRANSFERRED, FAILED}) {
            Map<String, Map<K, V>> stat = windowSetConverter(stat2win2sid2num.get(key), TO_STRING);
            if (EMITTED.equals(key) || TRANSFERRED.equals(key)) {
                stat = filterSysStreams(stat, includeSys);
            }
            Map<K, V> winStat = stat.get(window);
            long sum = 0;
            if (winStat != null) {
                for (V v : winStat.values()) {
                    sum += v.longValue();
                }
            }
            putKV(subRet, key, sum);
        }

        Map<String, Map<String, Double>> win2sid2compLat =
                windowSetConverter(getMapByKey(stat2win2sid2num, COMP_LATENCIES), TO_STRING);
        Map<String, Map<String, Long>> win2sid2acked =
                windowSetConverter(getMapByKey(stat2win2sid2num, ACKED), TO_STRING);
        subRet.putAll(aggSpoutLatAndCount(win2sid2compLat.get(window), win2sid2acked.get(window)));

        ret.put((String) getByKey(m, "comp-id"), subRet);
        return ret;
    }

    /**
     * merge accumulated bolt stats with pre-merged component stats
     *
     * @param accBoltStats accumulated bolt stats
     * @param boltStats    pre-merged component stats
     * @return merged stats
     */
    public static Map<String, Object> mergeAggCompStatsCompPageBolt(
            Map<String, Object> accBoltStats, Map<String, Object> boltStats) {
        Map<String, Object> ret = new HashMap<>();

        Map<List<String>, Map<String, ?>> accIn = getMapByKey(accBoltStats, CID_SID_TO_IN_STATS);
        Map<String, Map<String, ?>> accOut = getMapByKey(accBoltStats, SID_TO_OUT_STATS);
        Map<List<String>, Map<String, ?>> boltIn = getMapByKey(boltStats, CID_SID_TO_IN_STATS);
        Map<String, Map<String, ?>> boltOut = getMapByKey(boltStats, SID_TO_OUT_STATS);

        int numExecutors = getByKeyOr0(accBoltStats, NUM_EXECUTORS).intValue();
        putKV(ret, NUM_EXECUTORS, numExecutors + 1);
        putKV(ret, NUM_TASKS, sumOr0(
                getByKeyOr0(accBoltStats, NUM_TASKS), getByKeyOr0(boltStats, NUM_TASKS)));

        // (merge-with (partial merge-with sum-or-0) acc-out spout-out)
        putKV(ret, SID_TO_OUT_STATS, fullMergeWithSum(accOut, boltOut));
        // {component id -> metric -> value}, note that input may contain both long and double values
        putKV(ret, CID_SID_TO_IN_STATS, fullMergeWithSum(accIn, boltIn));

        long executed = sumStreamsLong(boltIn, EXECUTED);
        putKV(ret, EXECUTED, executed);

        Map<String, Object> executorStats = new HashMap<>();
        putKV(executorStats, EXECUTOR_ID, boltStats.get(EXECUTOR_ID));
        putKV(executorStats, UPTIME, boltStats.get(UPTIME));
        putKV(executorStats, HOST, boltStats.get(HOST));
        putKV(executorStats, PORT, boltStats.get(PORT));
        putKV(executorStats, CAPACITY, boltStats.get(CAPACITY));

        putKV(executorStats, EMITTED, sumStreamsLong(boltOut, EMITTED));
        putKV(executorStats, TRANSFERRED, sumStreamsLong(boltOut, TRANSFERRED));
        putKV(executorStats, ACKED, sumStreamsLong(boltIn, ACKED));
        putKV(executorStats, FAILED, sumStreamsLong(boltIn, FAILED));
        putKV(executorStats, EXECUTED, executed);

        if (executed > 0) {
            putKV(executorStats, EXEC_LATENCY, sumStreamsDouble(boltIn, EXEC_LAT_TOTAL) / executed);
            putKV(executorStats, PROC_LATENCY, sumStreamsDouble(boltIn, PROC_LAT_TOTAL) / executed);
        } else {
            putKV(executorStats, EXEC_LATENCY, null);
            putKV(executorStats, PROC_LATENCY, null);
        }
        List executorStatsList = ((List) getByKey(accBoltStats, EXECUTOR_STATS));
        executorStatsList.add(executorStats);
        putKV(ret, EXECUTOR_STATS, executorStatsList);

        return ret;
    }

    /**
     * merge accumulated bolt stats with pre-merged component stats
     */
    public static Map<String, Object> mergeAggCompStatsCompPageSpout(
            Map<String, Object> accSpoutStats, Map<String, Object> spoutStats) {
        Map<String, Object> ret = new HashMap<>();

        // {stream id -> metric -> value}, note that sid->out-stats may contain both long and double values
        Map<String, Map<String, ?>> accOut = getMapByKey(accSpoutStats, SID_TO_OUT_STATS);
        Map<String, Map<String, ?>> spoutOut = getMapByKey(spoutStats, SID_TO_OUT_STATS);

        int numExecutors = getByKeyOr0(accSpoutStats, NUM_EXECUTORS).intValue();
        putKV(ret, NUM_EXECUTORS, numExecutors + 1);
        putKV(ret, NUM_TASKS, sumOr0(
                getByKeyOr0(accSpoutStats, NUM_TASKS), getByKeyOr0(spoutStats, NUM_TASKS)));
        putKV(ret, SID_TO_OUT_STATS, fullMergeWithSum(accOut, spoutOut));

        Map executorStats = new HashMap();
        putKV(executorStats, EXECUTOR_ID, getByKey(spoutStats, EXECUTOR_ID));
        putKV(executorStats, UPTIME, getByKey(spoutStats, UPTIME));
        putKV(executorStats, HOST, getByKey(spoutStats, HOST));
        putKV(executorStats, PORT, getByKey(spoutStats, PORT));

        putKV(executorStats, EMITTED, sumStreamsLong(spoutOut, EMITTED));
        putKV(executorStats, TRANSFERRED, sumStreamsLong(spoutOut, TRANSFERRED));
        putKV(executorStats, FAILED, sumStreamsLong(spoutOut, FAILED));
        long acked = sumStreamsLong(spoutOut, ACKED);
        putKV(executorStats, ACKED, acked);
        if (acked > 0) {
            putKV(executorStats, COMP_LATENCY, sumStreamsDouble(spoutOut, COMP_LAT_TOTAL) / acked);
        } else {
            putKV(executorStats, COMP_LATENCY, null);
        }
        List executorStatsList = ((List) getByKey(accSpoutStats, EXECUTOR_STATS));
        executorStatsList.add(executorStats);
        putKV(ret, EXECUTOR_STATS, executorStatsList);

        return ret;
    }

    /**
     * merge accumulated bolt stats with new bolt stats
     *
     * @param accBoltStats accumulated bolt stats
     * @param boltStats    new input bolt stats
     * @return merged bolt stats
     */
    public static Map<String, Object> mergeAggCompStatsTopoPageBolt(Map<String, Object> accBoltStats,
                                                                    Map<String, Object> boltStats) {
        Map<String, Object> ret = new HashMap<>();

        Integer numExecutors = getByKeyOr0(accBoltStats, NUM_EXECUTORS).intValue();
        putKV(ret, NUM_EXECUTORS, numExecutors + 1);
        putKV(ret, NUM_TASKS,
                sumOr0(getByKeyOr0(accBoltStats, NUM_TASKS), getByKeyOr0(boltStats, NUM_TASKS)));
        putKV(ret, EMITTED,
                sumOr0(getByKeyOr0(accBoltStats, EMITTED), getByKeyOr0(boltStats, EMITTED)));
        putKV(ret, TRANSFERRED,
                sumOr0(getByKeyOr0(accBoltStats, TRANSFERRED), getByKeyOr0(boltStats, TRANSFERRED)));
        putKV(ret, EXEC_LAT_TOTAL,
                sumOr0(getByKeyOr0(accBoltStats, EXEC_LAT_TOTAL), getByKeyOr0(boltStats, EXEC_LAT_TOTAL)));
        putKV(ret, PROC_LAT_TOTAL,
                sumOr0(getByKeyOr0(accBoltStats, PROC_LAT_TOTAL), getByKeyOr0(boltStats, PROC_LAT_TOTAL)));
        putKV(ret, EXECUTED,
                sumOr0(getByKeyOr0(accBoltStats, EXECUTED), getByKeyOr0(boltStats, EXECUTED)));
        putKV(ret, ACKED,
                sumOr0(getByKeyOr0(accBoltStats, ACKED), getByKeyOr0(boltStats, ACKED)));
        putKV(ret, FAILED,
                sumOr0(getByKeyOr0(accBoltStats, FAILED), getByKeyOr0(boltStats, FAILED)));
        putKV(ret, CAPACITY,
                maxOr0(getByKeyOr0(accBoltStats, CAPACITY), getByKeyOr0(boltStats, CAPACITY)));

        return ret;
    }

    /**
     * merge accumulated bolt stats with new bolt stats
     */
    public static Map<String, Object> mergeAggCompStatsTopoPageSpout(Map<String, Object> accSpoutStats,
                                                                     Map<String, Object> spoutStats) {
        Map<String, Object> ret = new HashMap<>();

        Integer numExecutors = getByKeyOr0(accSpoutStats, NUM_EXECUTORS).intValue();
        putKV(ret, NUM_EXECUTORS, numExecutors + 1);
        putKV(ret, NUM_TASKS,
                sumOr0(getByKeyOr0(accSpoutStats, NUM_TASKS), getByKeyOr0(spoutStats, NUM_TASKS)));
        putKV(ret, EMITTED,
                sumOr0(getByKeyOr0(accSpoutStats, EMITTED), getByKeyOr0(spoutStats, EMITTED)));
        putKV(ret, TRANSFERRED,
                sumOr0(getByKeyOr0(accSpoutStats, TRANSFERRED), getByKeyOr0(spoutStats, TRANSFERRED)));
        putKV(ret, COMP_LAT_TOTAL,
                sumOr0(getByKeyOr0(accSpoutStats, COMP_LAT_TOTAL), getByKeyOr0(spoutStats, COMP_LAT_TOTAL)));
        putKV(ret, ACKED,
                sumOr0(getByKeyOr0(accSpoutStats, ACKED), getByKeyOr0(spoutStats, ACKED)));
        putKV(ret, FAILED,
                sumOr0(getByKeyOr0(accSpoutStats, FAILED), getByKeyOr0(spoutStats, FAILED)));

        return ret;
    }

    /**
     * A helper function that does the common work to aggregate stats of one
     * executor with the given map for the topology page.
     */
    public static Map<String, Object> aggTopoExecStats(
            String window, boolean includeSys, Map<String, Object> accStats, Map<String, Object> beat, String compType) {
        Map<String, Object> ret = new HashMap<>();

        Set workerSet = (Set) accStats.get(WORKERS_SET);
        Map bolt2stats = getMapByKey(accStats, BOLT_TO_STATS);
        Map spout2stats = getMapByKey(accStats, SPOUT_TO_STATS);
        Map win2emitted = getMapByKey(accStats, WIN_TO_EMITTED);
        Map win2transferred = getMapByKey(accStats, WIN_TO_TRANSFERRED);
        Map win2compLatWgtAvg = getMapByKey(accStats, WIN_TO_COMP_LAT_WGT_AVG);
        Map win2acked = getMapByKey(accStats, WIN_TO_ACKED);
        Map win2failed = getMapByKey(accStats, WIN_TO_FAILED);

        boolean isSpout = compType.equals(SPOUT);
        // component id -> stats
        Map<String, Object> cid2stats;
        if (isSpout) {
            cid2stats = aggPreMergeTopoPageSpout(beat, window, includeSys);
        } else {
            cid2stats = aggPreMergeTopoPageBolt(beat, window, includeSys);
        }

        Map stats = getMapByKey(beat, STATS);
        Map w2compLatWgtAvg, w2acked;
        Map compLatStats = getMapByKey(stats, COMP_LATENCIES);
        if (isSpout) { // agg spout stats
            Map mm = new HashMap();

            Map acked = getMapByKey(stats, ACKED);
            for (Object win : acked.keySet()) {
                mm.put(win, aggSpoutLatAndCount((Map) compLatStats.get(win), (Map) acked.get(win)));
            }
            mm = swapMapOrder(mm);
            w2compLatWgtAvg = getMapByKey(mm, COMP_LAT_TOTAL);
            w2acked = getMapByKey(mm, ACKED);
        } else {
            w2compLatWgtAvg = null;
            w2acked = aggregateCountStreams(getMapByKey(stats, ACKED));
        }

        workerSet.add(Lists.newArrayList(getByKey(beat, HOST), getByKey(beat, PORT)));
        putKV(ret, WORKERS_SET, workerSet);
        putKV(ret, BOLT_TO_STATS, bolt2stats);
        putKV(ret, SPOUT_TO_STATS, spout2stats);
        putKV(ret, WIN_TO_EMITTED, mergeWithSumLong(win2emitted, aggregateCountStreams(
                filterSysStreams(getMapByKey(stats, EMITTED), includeSys))));
        putKV(ret, WIN_TO_TRANSFERRED, mergeWithSumLong(win2transferred, aggregateCountStreams(
                filterSysStreams(getMapByKey(stats, TRANSFERRED), includeSys))));
        putKV(ret, WIN_TO_COMP_LAT_WGT_AVG, mergeWithSumDouble(win2compLatWgtAvg, w2compLatWgtAvg));

        //boolean isSpoutStat = SPOUT.equals(((Keyword) getByKey(stats, TYPE)).getName());
        putKV(ret, WIN_TO_ACKED, isSpout ? mergeWithSumLong(win2acked, w2acked) : win2acked);
        putKV(ret, WIN_TO_FAILED, isSpout ?
                mergeWithSumLong(aggregateCountStreams(getMapByKey(stats, FAILED)), win2failed) : win2failed);
        putKV(ret, TYPE, getByKey(stats, TYPE));

        // (merge-with merge-agg-comp-stats-topo-page-bolt/spout (acc-stats comp-key) cid->statk->num)
        // (acc-stats comp-key) ==> bolt2stats/spout2stats
        if (isSpout) {
            Set<String> spouts = new HashSet<>();
            spouts.addAll(spout2stats.keySet());
            spouts.addAll(cid2stats.keySet());

            Map<String, Object> mm = new HashMap<>();
            for (String spout : spouts) {
                mm.put(spout, mergeAggCompStatsTopoPageSpout((Map) spout2stats.get(spout), (Map) cid2stats.get(spout)));
            }
            putKV(ret, SPOUT_TO_STATS, mm);
        } else {
            Set<String> bolts = new HashSet<>();
            bolts.addAll(bolt2stats.keySet());
            bolts.addAll(cid2stats.keySet());

            Map<String, Object> mm = new HashMap<>();
            for (String bolt : bolts) {
                mm.put(bolt, mergeAggCompStatsTopoPageBolt((Map) bolt2stats.get(bolt), (Map) cid2stats.get(bolt)));
            }
            putKV(ret, BOLT_TO_STATS, mm);
        }

        return ret;
    }

    /**
     * aggregate topo executors stats
     * TODO: change clojure maps to java HashMap's when nimbus.clj is translated to java
     *
     * @param topologyId     topology id
     * @param exec2nodePort  executor -> host+port, note it's a clojure map
     * @param task2component task -> component, note it's a clojure map
     * @param beats          executor[start, end] -> executor heartbeat, note it's a java HashMap
     * @param topology       storm topology
     * @param window         the window to be aggregated
     * @param includeSys     whether to include system streams
     * @param clusterState   cluster state
     * @return TopologyPageInfo thrift structure
     */
    public static TopologyPageInfo aggTopoExecsStats(
            String topologyId, Map exec2nodePort, Map task2component, Map<List<Integer>, Map<String, Object>> beats,
            StormTopology topology, String window, boolean includeSys, IStormClusterState clusterState) {
        List<Map<String, Object>> beatList = extractDataFromHb(exec2nodePort, task2component, beats, includeSys, topology);
        Map<String, Object> topoStats = aggregateTopoStats(window, includeSys, beatList);
        return postAggregateTopoStats(task2component, exec2nodePort, topoStats, topologyId, clusterState);
    }

    public static Map<String, Object> aggregateTopoStats(String win, boolean includeSys, List<Map<String, Object>> heartbeats) {
        Map<String, Object> initVal = new HashMap<>();
        putKV(initVal, WORKERS_SET, new HashSet());
        putKV(initVal, BOLT_TO_STATS, new HashMap());
        putKV(initVal, SPOUT_TO_STATS, new HashMap());
        putKV(initVal, WIN_TO_EMITTED, new HashMap());
        putKV(initVal, WIN_TO_TRANSFERRED, new HashMap());
        putKV(initVal, WIN_TO_COMP_LAT_WGT_AVG, new HashMap());
        putKV(initVal, WIN_TO_ACKED, new HashMap());
        putKV(initVal, WIN_TO_FAILED, new HashMap());

        for (Map<String, Object> heartbeat : heartbeats) {
            String compType = (String) getByKey(heartbeat, TYPE);
            initVal = aggTopoExecStats(win, includeSys, initVal, heartbeat, compType);
        }

        return initVal;
    }

    public static TopologyPageInfo postAggregateTopoStats(Map task2comp, Map exec2nodePort, Map<String, Object> accData,
                                                          String topologyId, IStormClusterState clusterState) {
        TopologyPageInfo ret = new TopologyPageInfo(topologyId);

        ret.set_num_tasks(task2comp.size());
        ret.set_num_workers(((Set) getByKey(accData, WORKERS_SET)).size());
        ret.set_num_executors(exec2nodePort != null ? exec2nodePort.size() : 0);

        Map bolt2stats = getMapByKey(accData, BOLT_TO_STATS);
        Map<String, ComponentAggregateStats> aggBolt2stats = new HashMap<>();
        for (Object o : bolt2stats.entrySet()) {
            Map.Entry e = (Map.Entry) o;
            String id = (String) e.getKey();
            Map m = (Map) e.getValue();
            long executed = getByKeyOr0(m, EXECUTED).longValue();
            if (executed > 0) {
                double execLatencyTotal = getByKeyOr0(m, EXEC_LAT_TOTAL).doubleValue();
                putKV(m, EXEC_LATENCY, execLatencyTotal / executed);

                double procLatencyTotal = getByKeyOr0(m, PROC_LAT_TOTAL).doubleValue();
                putKV(m, PROC_LATENCY, procLatencyTotal / executed);
            }
            remove(m, EXEC_LAT_TOTAL);
            remove(m, PROC_LAT_TOTAL);
            putKV(m, "last-error", getLastError(clusterState, topologyId, id));

            aggBolt2stats.put(id, thriftifyBoltAggStats(m));
        }

        Map spout2stats = getMapByKey(accData, SPOUT_TO_STATS);
        Map<String, ComponentAggregateStats> aggSpout2stats = new HashMap<>();
        for (Object o : spout2stats.entrySet()) {
            Map.Entry e = (Map.Entry) o;
            String id = (String) e.getKey();
            Map m = (Map) e.getValue();
            long acked = getByKeyOr0(m, ACKED).longValue();
            if (acked > 0) {
                double compLatencyTotal = getByKeyOr0(m, COMP_LAT_TOTAL).doubleValue();
                putKV(m, COMP_LATENCY, compLatencyTotal / acked);
            }
            remove(m, COMP_LAT_TOTAL);
            putKV(m, "last-error", getLastError(clusterState, topologyId, id));

            aggSpout2stats.put(id, thriftifySpoutAggStats(m));
        }

        TopologyStats topologyStats = new TopologyStats();
        topologyStats.set_window_to_acked(mapKeyStr(getMapByKey(accData, WIN_TO_ACKED)));
        topologyStats.set_window_to_emitted(mapKeyStr(getMapByKey(accData, WIN_TO_EMITTED)));
        topologyStats.set_window_to_failed(mapKeyStr(getMapByKey(accData, WIN_TO_FAILED)));
        topologyStats.set_window_to_transferred(mapKeyStr(getMapByKey(accData, WIN_TO_TRANSFERRED)));
        topologyStats.set_window_to_complete_latencies_ms(computeWeightedAveragesPerWindow(
                accData, WIN_TO_COMP_LAT_WGT_AVG, WIN_TO_ACKED));

        ret.set_topology_stats(topologyStats);
        ret.set_id_to_spout_agg_stats(aggSpout2stats);
        ret.set_id_to_bolt_agg_stats(aggBolt2stats);

        return ret;
    }

    /**
     * aggregate bolt stats
     *
     * @param statsSeq   a seq of ExecutorStats
     * @param includeSys whether to include system streams
     * @return aggregated bolt stats: {metric -> win -> global stream id -> value}
     */
    public static <T> Map<String, Map> aggregateBoltStats(List<ExecutorSummary> statsSeq, boolean includeSys) {
        Map<String, Map> ret = new HashMap<>();

        Map<String, Map<String, Map<T, Long>>> commonStats = aggregateCommonStats(statsSeq);
        // filter sys streams if necessary
        commonStats = preProcessStreamSummary(commonStats, includeSys);

        List<Map<String, Map<GlobalStreamId, Long>>> acked = new ArrayList<>();
        List<Map<String, Map<GlobalStreamId, Long>>> failed = new ArrayList<>();
        List<Map<String, Map<GlobalStreamId, Long>>> executed = new ArrayList<>();
        List<Map<String, Map<GlobalStreamId, Double>>> processLatencies = new ArrayList<>();
        List<Map<String, Map<GlobalStreamId, Double>>> executeLatencies = new ArrayList<>();
        for (ExecutorSummary summary : statsSeq) {
            ExecutorStats stat = summary.get_stats();
            acked.add(stat.get_specific().get_bolt().get_acked());
            failed.add(stat.get_specific().get_bolt().get_failed());
            executed.add(stat.get_specific().get_bolt().get_executed());
            processLatencies.add(stat.get_specific().get_bolt().get_process_ms_avg());
            executeLatencies.add(stat.get_specific().get_bolt().get_execute_ms_avg());
        }
        mergeMaps(ret, commonStats);
        putKV(ret, ACKED, aggregateCounts(acked));
        putKV(ret, FAILED, aggregateCounts(failed));
        putKV(ret, EXECUTED, aggregateCounts(executed));
        putKV(ret, PROC_LATENCIES, aggregateAverages(processLatencies, acked));
        putKV(ret, EXEC_LATENCIES, aggregateAverages(executeLatencies, executed));

        return ret;
    }

    /**
     * aggregate spout stats
     *
     * @param statsSeq   a seq of ExecutorStats
     * @param includeSys whether to include system streams
     * @return aggregated spout stats: {metric -> win -> global stream id -> value}
     */
    public static Map<String, Map> aggregateSpoutStats(List<ExecutorSummary> statsSeq, boolean includeSys) {
        // actually Map<String, Map<String, Map<String, Long/Double>>>
        Map<String, Map> ret = new HashMap<>();

        Map<String, Map<String, Map<String, Long>>> commonStats = aggregateCommonStats(statsSeq);
        // filter sys streams if necessary
        commonStats = preProcessStreamSummary(commonStats, includeSys);

        List<Map<String, Map<String, Long>>> acked = new ArrayList<>();
        List<Map<String, Map<String, Long>>> failed = new ArrayList<>();
        List<Map<String, Map<String, Double>>> completeLatencies = new ArrayList<>();
        for (ExecutorSummary summary : statsSeq) {
            ExecutorStats stats = summary.get_stats();
            acked.add(stats.get_specific().get_spout().get_acked());
            failed.add(stats.get_specific().get_spout().get_failed());
            completeLatencies.add(stats.get_specific().get_spout().get_complete_ms_avg());
        }
        ret.putAll(commonStats);
        putKV(ret, ACKED, aggregateCounts(acked));
        putKV(ret, FAILED, aggregateCounts(failed));
        putKV(ret, COMP_LATENCIES, aggregateAverages(completeLatencies, acked));

        return ret;
    }

    /**
     * aggregate common stats from a spout/bolt, called in aggregateSpoutStats/aggregateBoltStats
     */
    public static <T> Map<String, Map<String, Map<T, Long>>> aggregateCommonStats(List<ExecutorSummary> statsSeq) {
        Map<String, Map<String, Map<T, Long>>> ret = new HashMap<>();

        List<Map<String, Map<String, Long>>> emitted = new ArrayList<>();
        List<Map<String, Map<String, Long>>> transferred = new ArrayList<>();
        for (ExecutorSummary summ : statsSeq) {
            emitted.add(summ.get_stats().get_emitted());
            transferred.add(summ.get_stats().get_transferred());
        }
        putKV(ret, EMITTED, aggregateCounts(emitted));
        putKV(ret, TRANSFERRED, aggregateCounts(transferred));

        return ret;
    }

    /**
     * filter system streams of aggregated spout/bolt stats if necessary
     */
    public static <T> Map<String, Map<String, Map<T, Long>>> preProcessStreamSummary(
            Map<String, Map<String, Map<T, Long>>> streamSummary, boolean includeSys) {
        Map<String, Map<T, Long>> emitted = getMapByKey(streamSummary, EMITTED);
        Map<String, Map<T, Long>> transferred = getMapByKey(streamSummary, TRANSFERRED);

        putKV(streamSummary, EMITTED, filterSysStreams(emitted, includeSys));
        putKV(streamSummary, TRANSFERRED, filterSysStreams(transferred, includeSys));

        return streamSummary;
    }

    /**
     * aggregate count streams by window
     *
     * @param stats a Map of value: {win -> stream -> value}
     * @return a Map of value: {win -> value}
     */
    public static <K, V extends Number> Map<String, Long> aggregateCountStreams(
            Map<String, Map<K, V>> stats) {
        Map<String, Long> ret = new HashMap<>();
        for (Map.Entry<String, Map<K, V>> entry : stats.entrySet()) {
            Map<K, V> value = entry.getValue();
            long sum = 0l;
            for (V num : value.values()) {
                sum += num.longValue();
            }
            ret.put(entry.getKey(), sum);
        }
        return ret;
    }

    /**
     * compute an weighted average from a list of average maps and a corresponding count maps
     * extracted from a list of ExecutorSummary
     *
     * @param avgSeq   a list of {win -> global stream id -> avg value}
     * @param countSeq a list of {win -> global stream id -> count value}
     * @return a Map of {win -> global stream id -> weighted avg value}
     */
    public static <K> Map<String, Map<K, Double>> aggregateAverages(List<Map<String, Map<K, Double>>> avgSeq,
                                                                    List<Map<String, Map<K, Long>>> countSeq) {
        Map<String, Map<K, Double>> ret = new HashMap<>();

        Map<String, Map<K, List>> expands = expandAveragesSeq(avgSeq, countSeq);
        for (Map.Entry<String, Map<K, List>> entry : expands.entrySet()) {
            String k = entry.getKey();

            Map<K, Double> tmp = new HashMap<>();
            Map<K, List> inner = entry.getValue();
            for (K kk : inner.keySet()) {
                List vv = inner.get(kk);
                tmp.put(kk, valAvg(((Number) vv.get(0)).doubleValue(), ((Number) vv.get(1)).longValue()));
            }
            ret.put(k, tmp);
        }

        return ret;
    }

    /**
     * aggregate weighted average of all streams
     *
     * @param avgs   a Map of {win -> stream -> average value}
     * @param counts a Map of {win -> stream -> count value}
     * @return a Map of {win -> aggregated value}
     */
    public static <K> Map<String, Double> aggregateAvgStreams(Map<String, Map<K, Double>> avgs,
                                                              Map<String, Map<K, Long>> counts) {
        Map<String, Double> ret = new HashMap<>();

        Map<String, Map<K, List>> expands = expandAverages(avgs, counts);
        for (Map.Entry<String, Map<K, List>> entry : expands.entrySet()) {
            String win = entry.getKey();

            double avgTotal = 0.0;
            long cntTotal = 0l;
            Map<K, List> inner = entry.getValue();
            for (K kk : inner.keySet()) {
                List vv = inner.get(kk);
                avgTotal += ((Number) vv.get(0)).doubleValue();
                cntTotal += ((Number) vv.get(1)).longValue();
            }
            ret.put(win, valAvg(avgTotal, cntTotal));
        }

        return ret;
    }

    /**
     * aggregates spout stream stats, returns a Map of {metric -> win -> aggregated value}
     */
    public static Map<String, Map> spoutStreamsStats(List<ExecutorSummary> summs, boolean includeSys) {
        if (summs == null) {
            return new HashMap<>();
        }
        // filter ExecutorSummary's with empty stats
        List<ExecutorSummary> statsSeq = getFilledStats(summs);
        return aggregateSpoutStreams(aggregateSpoutStats(statsSeq, includeSys));
    }

    /**
     * aggregates bolt stream stats, returns a Map of {metric -> win -> aggregated value}
     */
    public static Map<String, Map> boltStreamsStats(List<ExecutorSummary> summs, boolean includeSys) {
        if (summs == null) {
            return new HashMap<>();
        }
        List<ExecutorSummary> statsSeq = getFilledStats(summs);
        return aggregateBoltStreams(aggregateBoltStats(statsSeq, includeSys));
    }

    /**
     * aggregate all spout streams
     *
     * @param stats a Map of {metric -> win -> stream id -> value}
     * @return a Map of {metric -> win -> aggregated value}
     */
    public static Map<String, Map> aggregateSpoutStreams(Map<String, Map> stats) {
        // actual ret is Map<String, Map<String, Long/Double>>
        Map<String, Map> ret = new HashMap<>();
        putKV(ret, ACKED, aggregateCountStreams(getMapByKey(stats, ACKED)));
        putKV(ret, FAILED, aggregateCountStreams(getMapByKey(stats, FAILED)));
        putKV(ret, EMITTED, aggregateCountStreams(getMapByKey(stats, EMITTED)));
        putKV(ret, TRANSFERRED, aggregateCountStreams(getMapByKey(stats, TRANSFERRED)));
        putKV(ret, COMP_LATENCIES, aggregateAvgStreams(
                getMapByKey(stats, COMP_LATENCIES), getMapByKey(stats, ACKED)));
        return ret;
    }

    /**
     * aggregate all bolt streams
     *
     * @param stats a Map of {metric -> win -> stream id -> value}
     * @return a Map of {metric -> win -> aggregated value}
     */
    public static Map<String, Map> aggregateBoltStreams(Map<String, Map> stats) {
        Map<String, Map> ret = new HashMap<>();
        putKV(ret, ACKED, aggregateCountStreams(getMapByKey(stats, ACKED)));
        putKV(ret, FAILED, aggregateCountStreams(getMapByKey(stats, FAILED)));
        putKV(ret, EMITTED, aggregateCountStreams(getMapByKey(stats, EMITTED)));
        putKV(ret, TRANSFERRED, aggregateCountStreams(getMapByKey(stats, TRANSFERRED)));
        putKV(ret, EXECUTED, aggregateCountStreams(getMapByKey(stats, EXECUTED)));
        putKV(ret, PROC_LATENCIES, aggregateAvgStreams(
                getMapByKey(stats, PROC_LATENCIES), getMapByKey(stats, ACKED)));
        putKV(ret, EXEC_LATENCIES, aggregateAvgStreams(
                getMapByKey(stats, EXEC_LATENCIES), getMapByKey(stats, EXECUTED)));
        return ret;
    }

    /**
     * aggregate windowed stats from a bolt executor stats with a Map of accumulated stats
     */
    public static Map<String, Object> aggBoltExecWinStats(
            Map<String, Object> accStats, Map<String, Object> newStats, boolean includeSys) {
        Map<String, Object> ret = new HashMap<>();

        Map<String, Map<String, Number>> m = new HashMap<>();
        for (Object win : getMapByKey(newStats, EXECUTED).keySet()) {
            m.put((String) win, aggBoltLatAndCount(
                    (Map) (getMapByKey(newStats, EXEC_LATENCIES)).get(win),
                    (Map) (getMapByKey(newStats, PROC_LATENCIES)).get(win),
                    (Map) (getMapByKey(newStats, EXECUTED)).get(win)));
        }
        m = swapMapOrder(m);

        Map<String, Double> win2execLatWgtAvg = getMapByKey(m, EXEC_LAT_TOTAL);
        Map<String, Double> win2procLatWgtAvg = getMapByKey(m, PROC_LAT_TOTAL);
        Map<String, Long> win2executed = getMapByKey(m, EXECUTED);

        Map<String, Map<String, Long>> emitted = getMapByKey(newStats, EMITTED);
        Map<String, Long> win2emitted = mergeWithSumLong(aggregateCountStreams(filterSysStreams(emitted, includeSys)),
                getMapByKey(accStats, WIN_TO_EMITTED));
        putKV(ret, WIN_TO_EMITTED, win2emitted);

        Map<String, Map<String, Long>> transferred = getMapByKey(newStats, TRANSFERRED);
        Map<String, Long> win2transferred = mergeWithSumLong(aggregateCountStreams(filterSysStreams(transferred, includeSys)),
                getMapByKey(accStats, WIN_TO_TRANSFERRED));
        putKV(ret, WIN_TO_TRANSFERRED, win2transferred);

        putKV(ret, WIN_TO_EXEC_LAT_WGT_AVG, mergeWithSumDouble(
                getMapByKey(accStats, WIN_TO_EXEC_LAT_WGT_AVG), win2execLatWgtAvg));
        putKV(ret, WIN_TO_PROC_LAT_WGT_AVG, mergeWithSumDouble(
                getMapByKey(accStats, WIN_TO_PROC_LAT_WGT_AVG), win2procLatWgtAvg));
        putKV(ret, WIN_TO_EXECUTED, mergeWithSumLong(
                getMapByKey(accStats, WIN_TO_EXECUTED), win2executed));
        putKV(ret, WIN_TO_ACKED, mergeWithSumLong(
                aggregateCountStreams(getMapByKey(newStats, ACKED)), getMapByKey(accStats, WIN_TO_ACKED)));
        putKV(ret, WIN_TO_FAILED, mergeWithSumLong(
                aggregateCountStreams(getMapByKey(newStats, FAILED)), getMapByKey(accStats, WIN_TO_FAILED)));

        return ret;
    }

    /**
     * aggregate windowed stats from a spout executor stats with a Map of accumulated stats
     */
    public static Map<String, Object> aggSpoutExecWinStats(
            Map<String, Object> accStats, Map<String, Object> beat, boolean includeSys) {
        Map<String, Object> ret = new HashMap<>();

        Map<String, Map<String, Number>> m = new HashMap<>();
        for (Object win : getMapByKey(beat, ACKED).keySet()) {
            m.put((String) win, aggSpoutLatAndCount(
                    (Map<String, Double>) (getMapByKey(beat, COMP_LATENCIES)).get(win),
                    (Map<String, Long>) (getMapByKey(beat, ACKED)).get(win)));
        }
        m = swapMapOrder(m);

        Map<String, Double> win2compLatWgtAvg = getMapByKey(m, COMP_LAT_TOTAL);
        Map<String, Long> win2acked = getMapByKey(m, ACKED);

        Map<String, Map<String, Long>> emitted = getMapByKey(beat, EMITTED);
        Map<String, Long> win2emitted = mergeWithSumLong(aggregateCountStreams(filterSysStreams(emitted, includeSys)),
                getMapByKey(accStats, WIN_TO_EMITTED));
        putKV(ret, WIN_TO_EMITTED, win2emitted);

        Map<String, Map<String, Long>> transferred = getMapByKey(beat, TRANSFERRED);
        Map<String, Long> win2transferred = mergeWithSumLong(aggregateCountStreams(filterSysStreams(transferred, includeSys)),
                getMapByKey(accStats, WIN_TO_TRANSFERRED));
        putKV(ret, WIN_TO_TRANSFERRED, win2transferred);

        putKV(ret, WIN_TO_COMP_LAT_WGT_AVG, mergeWithSumDouble(
                getMapByKey(accStats, WIN_TO_COMP_LAT_WGT_AVG), win2compLatWgtAvg));
        putKV(ret, WIN_TO_ACKED, mergeWithSumLong(
                getMapByKey(accStats, WIN_TO_ACKED), win2acked));
        putKV(ret, WIN_TO_FAILED, mergeWithSumLong(
                aggregateCountStreams(getMapByKey(beat, FAILED)), getMapByKey(accStats, WIN_TO_FAILED)));

        return ret;
    }


    /**
     * aggregate a list of count maps into one map
     *
     * @param countsSeq a seq of {win -> GlobalStreamId -> value}
     */
    public static <T> Map<String, Map<T, Long>> aggregateCounts(List<Map<String, Map<T, Long>>> countsSeq) {
        Map<String, Map<T, Long>> ret = new HashMap<>();
        for (Map<String, Map<T, Long>> counts : countsSeq) {
            for (Map.Entry<String, Map<T, Long>> entry : counts.entrySet()) {
                String win = entry.getKey();
                Map<T, Long> stream2count = entry.getValue();

                if (!ret.containsKey(win)) {
                    ret.put(win, stream2count);
                } else {
                    Map<T, Long> existing = ret.get(win);
                    for (Map.Entry<T, Long> subEntry : stream2count.entrySet()) {
                        T stream = subEntry.getKey();
                        if (!existing.containsKey(stream)) {
                            existing.put(stream, subEntry.getValue());
                        } else {
                            existing.put(stream, subEntry.getValue() + existing.get(stream));
                        }
                    }
                }
            }
        }
        return ret;
    }

    public static Map<String, Object> aggregateCompStats(
            String window, boolean includeSys, List<Map<String, Object>> beats, String compType) {
        boolean isSpout = SPOUT.equals(compType);

        Map<String, Object> initVal = new HashMap<>();
        putKV(initVal, WIN_TO_ACKED, new HashMap());
        putKV(initVal, WIN_TO_FAILED, new HashMap());
        putKV(initVal, WIN_TO_EMITTED, new HashMap());
        putKV(initVal, WIN_TO_TRANSFERRED, new HashMap());

        Map<String, Object> stats = new HashMap();
        putKV(stats, EXECUTOR_STATS, new ArrayList());
        putKV(stats, SID_TO_OUT_STATS, new HashMap());
        if (isSpout) {
            putKV(initVal, TYPE, SPOUT);
            putKV(initVal, WIN_TO_COMP_LAT_WGT_AVG, new HashMap());
        } else {
            putKV(initVal, TYPE, BOLT);
            putKV(initVal, WIN_TO_EXECUTED, new HashMap());
            putKV(stats, CID_SID_TO_IN_STATS, new HashMap());
            putKV(initVal, WIN_TO_EXEC_LAT_WGT_AVG, new HashMap());
            putKV(initVal, WIN_TO_PROC_LAT_WGT_AVG, new HashMap());
        }
        putKV(initVal, STATS, stats);

        // iterate through all executor heartbeats
        for (Map<String, Object> beat : beats) {
            initVal = aggCompExecStats(window, includeSys, initVal, beat, compType);
        }

        return initVal;
    }

    /**
     * Combines the aggregate stats of one executor with the given map, selecting
     * the appropriate window and including system components as specified.
     */
    public static Map<String, Object> aggCompExecStats(String window, boolean includeSys, Map<String, Object> accStats,
                                                       Map<String, Object> beat, String compType) {
        Map<String, Object> ret = new HashMap<>();
        if (SPOUT.equals(compType)) {
            ret.putAll(aggSpoutExecWinStats(accStats, getMapByKey(beat, STATS), includeSys));
            putKV(ret, STATS, mergeAggCompStatsCompPageSpout(
                    getMapByKey(accStats, STATS),
                    aggPreMergeCompPageSpout(beat, window, includeSys)));
        } else {
            ret.putAll(aggBoltExecWinStats(accStats, getMapByKey(beat, STATS), includeSys));
            putKV(ret, STATS, mergeAggCompStatsCompPageBolt(
                    getMapByKey(accStats, STATS),
                    aggPreMergeCompPageBolt(beat, window, includeSys)));
        }
        putKV(ret, TYPE, compType);

        return ret;
    }

    /**
     * post aggregate component stats:
     * 1. computes execute-latency/process-latency from execute/process latency total
     * 2. computes windowed weight avgs
     * 3. transform Map keys
     *
     * @param compStats accumulated comp stats
     */
    public static Map<String, Object> postAggregateCompStats(Map<String, Object> compStats) {
        Map<String, Object> ret = new HashMap<>();

        String compType = (String) compStats.get(TYPE);
        Map stats = getMapByKey(compStats, STATS);
        Integer numTasks = getByKeyOr0(stats, NUM_TASKS).intValue();
        Integer numExecutors = getByKeyOr0(stats, NUM_EXECUTORS).intValue();
        Map outStats = getMapByKey(stats, SID_TO_OUT_STATS);

        putKV(ret, TYPE, compType);
        putKV(ret, NUM_TASKS, numTasks);
        putKV(ret, NUM_EXECUTORS, numExecutors);
        putKV(ret, EXECUTOR_STATS, getByKey(stats, EXECUTOR_STATS));
        putKV(ret, WIN_TO_EMITTED, mapKeyStr(getMapByKey(compStats, WIN_TO_EMITTED)));
        putKV(ret, WIN_TO_TRANSFERRED, mapKeyStr(getMapByKey(compStats, WIN_TO_TRANSFERRED)));
        putKV(ret, WIN_TO_ACKED, mapKeyStr(getMapByKey(compStats, WIN_TO_ACKED)));
        putKV(ret, WIN_TO_FAILED, mapKeyStr(getMapByKey(compStats, WIN_TO_FAILED)));

        if (BOLT.equals(compType)) {
            Map inStats = getMapByKey(stats, CID_SID_TO_IN_STATS);

            Map inStats2 = new HashMap();
            for (Object o : inStats.entrySet()) {
                Map.Entry e = (Map.Entry) o;
                Object k = e.getKey();
                Map v = (Map) e.getValue();
                long executed = getByKeyOr0(v, EXECUTED).longValue();
                if (executed > 0) {
                    double executeLatencyTotal = getByKeyOr0(v, EXEC_LAT_TOTAL).doubleValue();
                    double processLatencyTotal = getByKeyOr0(v, PROC_LAT_TOTAL).doubleValue();
                    putKV(v, EXEC_LATENCY, executeLatencyTotal / executed);
                    putKV(v, PROC_LATENCY, processLatencyTotal / executed);
                } else {
                    putKV(v, EXEC_LATENCY, 0.0);
                    putKV(v, PROC_LATENCY, 0.0);
                }
                remove(v, EXEC_LAT_TOTAL);
                remove(v, PROC_LAT_TOTAL);
                inStats2.put(k, v);
            }
            putKV(ret, CID_SID_TO_IN_STATS, inStats2);

            putKV(ret, SID_TO_OUT_STATS, outStats);
            putKV(ret, WIN_TO_EXECUTED, mapKeyStr(getMapByKey(compStats, WIN_TO_EXECUTED)));
            putKV(ret, WIN_TO_EXEC_LAT, computeWeightedAveragesPerWindow(
                    compStats, WIN_TO_EXEC_LAT_WGT_AVG, WIN_TO_EXECUTED));
            putKV(ret, WIN_TO_PROC_LAT, computeWeightedAveragesPerWindow(
                    compStats, WIN_TO_PROC_LAT_WGT_AVG, WIN_TO_EXECUTED));
        } else {
            Map outStats2 = new HashMap();
            for (Object o : outStats.entrySet()) {
                Map.Entry e = (Map.Entry) o;
                Object k = e.getKey();
                Map v = (Map) e.getValue();
                long acked = getByKeyOr0(v, ACKED).longValue();
                if (acked > 0) {
                    double compLatencyTotal = getByKeyOr0(v, COMP_LAT_TOTAL).doubleValue();
                    putKV(v, COMP_LATENCY, compLatencyTotal / acked);
                } else {
                    putKV(v, COMP_LATENCY, 0.0);
                }
                remove(v, COMP_LAT_TOTAL);
                outStats2.put(k, v);
            }
            putKV(ret, SID_TO_OUT_STATS, outStats2);
            putKV(ret, WIN_TO_COMP_LAT, computeWeightedAveragesPerWindow(
                    compStats, WIN_TO_COMP_LAT_WGT_AVG, WIN_TO_ACKED));
        }

        return ret;
    }

    /**
     * aggregate component executor stats
     *
     * @param exec2hostPort  a Map of {executor -> host+port}, note it's a clojure map
     * @param task2component a Map of {task id -> component}, note it's a clojure map
     * @param beats          a converted HashMap of executor heartbeats, {executor -> heartbeat}
     * @param window         specified window
     * @param includeSys     whether to include system streams
     * @param topologyId     topology id
     * @param topology       storm topology
     * @param componentId    component id
     * @return ComponentPageInfo thrift structure
     */
    public static ComponentPageInfo aggCompExecsStats(
            Map exec2hostPort, Map task2component, Map<List<Integer>, Map<String, Object>> beats,
            String window, boolean includeSys, String topologyId, StormTopology topology, String componentId) {

        List<Map<String, Object>> beatList =
                extractDataFromHb(exec2hostPort, task2component, beats, includeSys, topology, componentId);
        Map<String, Object> compStats = aggregateCompStats(window, includeSys, beatList, componentType(topology, componentId));
        compStats = postAggregateCompStats(compStats);
        return thriftifyCompPageData(topologyId, topology, componentId, compStats);
    }


    // =====================================================================================
    // convert thrift stats to java maps
    // =====================================================================================

    /**
     * convert thrift executor heartbeats into a java HashMap
     */
    public static Map<List<Integer>, Map<String, Object>> convertExecutorBeats(Map<ExecutorInfo, ExecutorBeat> beats) {
        Map<List<Integer>, Map<String, Object>> ret = new HashMap<>();
        for (Map.Entry<ExecutorInfo, ExecutorBeat> beat : beats.entrySet()) {
            ExecutorInfo executorInfo = beat.getKey();
            ExecutorBeat executorBeat = beat.getValue();
            ret.put(Lists.newArrayList(executorInfo.get_task_start(), executorInfo.get_task_end()),
                    convertZkExecutorHb(executorBeat));
        }

        return ret;
    }

    /**
     * convert thrift ExecutorBeat into a java HashMap
     */
    public static Map<String, Object> convertZkExecutorHb(ExecutorBeat beat) {
        Map<String, Object> ret = new HashMap<>();
        if (beat != null) {
            ret.put(TIME_SECS, beat.getTimeSecs());
            ret.put(UPTIME, beat.getUptime());
            ret.put(STATS, convertExecutorStats(beat.getStats()));
        }

        return ret;
    }

    /**
     * convert a thrift worker heartbeat into a java HashMap
     *
     * @param workerHb
     * @return
     */
    public static Map<String, Object> convertZkWorkerHb(ClusterWorkerHeartbeat workerHb) {
        Map<String, Object> ret = new HashMap<>();
        if (workerHb != null) {
            ret.put("storm-id", workerHb.get_storm_id());
            ret.put(EXECUTOR_STATS, convertExecutorsStats(workerHb.get_executor_stats()));
            ret.put(UPTIME, workerHb.get_uptime_secs());
            ret.put(TIME_SECS, workerHb.get_time_secs());
        }
        return ret;
    }

    /**
     * convert executors stats into a HashMap, note that ExecutorStats are remained unchanged
     */
    public static Map<List<Integer>, ExecutorStats> convertExecutorsStats(Map<ExecutorInfo, ExecutorStats> stats) {
        Map<List<Integer>, ExecutorStats> ret = new HashMap<>();
        for (Map.Entry<ExecutorInfo, ExecutorStats> entry : stats.entrySet()) {
            ExecutorInfo executorInfo = entry.getKey();
            ExecutorStats executorStats = entry.getValue();

            ret.put(Lists.newArrayList(executorInfo.get_task_start(), executorInfo.get_task_end()),
                    executorStats);
        }
        return ret;
    }

    /**
     * convert thrift ExecutorStats structure into a java HashMap
     */
    public static Map<String, Object> convertExecutorStats(ExecutorStats stats) {
        Map<String, Object> ret = new HashMap<>();

        putKV(ret, EMITTED, stats.get_emitted());
        putKV(ret, TRANSFERRED, stats.get_transferred());
        putKV(ret, RATE, stats.get_rate());

        if (stats.get_specific().is_set_bolt()) {
            ret.putAll(convertSpecificStats(stats.get_specific().get_bolt()));
            putKV(ret, TYPE, BOLT);
        } else {
            ret.putAll(convertSpecificStats(stats.get_specific().get_spout()));
            putKV(ret, TYPE, SPOUT);
        }

        return ret;
    }

    private static Map<String, Object> convertSpecificStats(SpoutStats stats) {
        Map<String, Object> ret = new HashMap<>();
        putKV(ret, ACKED, stats.get_acked());
        putKV(ret, FAILED, stats.get_failed());
        putKV(ret, COMP_LATENCIES, stats.get_complete_ms_avg());

        return ret;
    }

    private static Map<String, Object> convertSpecificStats(BoltStats stats) {
        Map<String, Object> ret = new HashMap<>();

        Map acked = windowSetConverter(stats.get_acked(), FROM_GSID, IDENTITY);
        Map failed = windowSetConverter(stats.get_failed(), FROM_GSID, IDENTITY);
        Map processAvg = windowSetConverter(stats.get_process_ms_avg(), FROM_GSID, IDENTITY);
        Map executed = windowSetConverter(stats.get_executed(), FROM_GSID, IDENTITY);
        Map executeAvg = windowSetConverter(stats.get_execute_ms_avg(), FROM_GSID, IDENTITY);

        putKV(ret, ACKED, acked);
        putKV(ret, FAILED, failed);
        putKV(ret, PROC_LATENCIES, processAvg);
        putKV(ret, EXECUTED, executed);
        putKV(ret, EXEC_LATENCIES, executeAvg);

        return ret;
    }

    /**
     * extract a list of host port info for specified component
     *
     * @param exec2hostPort  {executor -> host+port}, note it's a clojure map
     * @param task2component {task id -> component}, note it's a clojure map
     * @param includeSys     whether to include system streams
     * @param compId         component id
     * @return a list of host+port
     */
    public static List<Map<String, Object>> extractNodeInfosFromHbForComp(
            Map exec2hostPort, Map task2component, boolean includeSys, String compId) {
        List<Map<String, Object>> ret = new ArrayList<>();

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
            Map<String, Object> m = new HashMap<>();
            putKV(m, HOST, hostPort.get(0));
            putKV(m, PORT, hostPort.get(1));
            ret.add(m);
        }

        return ret;
    }


    // =====================================================================================
    // heartbeats related
    // =====================================================================================

    /**
     * update all executor heart beats
     * TODO: should move this method to nimbus when nimbus.clj is translated
     *
     * @param cache         existing heart beats cache
     * @param executorBeats new heart beats
     * @param executors     all executors
     * @param timeout       timeout
     * @return a HashMap of updated executor heart beats
     */
    public static Map<List<Integer>, Object> updateHeartbeatCache(Map<List<Integer>, Map<String, Object>> cache,
                                                                  Map<List<Integer>, Map<String, Object>> executorBeats,
                                                                  Set<List<Integer>> executors, Integer timeout) {
        Map<List<Integer>, Object> ret = new HashMap<>();
        if (cache == null && executorBeats == null) {
            return ret;
        }

        if (cache == null) {
            cache = new HashMap<>();
        }
        if (executorBeats == null) {
            executorBeats = new HashMap<>();
        }

        for (List<Integer> executor : executors) {
            ret.put(executor, updateExecutorCache(cache.get(executor), executorBeats.get(executor), timeout));
        }

        return ret;
    }

    // TODO: should move this method to nimbus when nimbus.clj is translated
    public static Map<String, Object> updateExecutorCache(
            Map<String, Object> currBeat, Map<String, Object> newBeat, Integer timeout) {
        Map<String, Object> ret = new HashMap<>();

        Integer lastNimbusTime = null, lastReportedTime = null;
        if (currBeat != null) {
            lastNimbusTime = (Integer) currBeat.get("nimbus-time");
            lastReportedTime = (Integer) currBeat.get("executor-reported-time");
        }

        Integer reportedTime = null;
        if (newBeat != null) {
            reportedTime = (Integer) newBeat.get(TIME_SECS);
        }

        if (reportedTime == null) {
            if (lastReportedTime != null) {
                reportedTime = lastReportedTime;
            } else {
                reportedTime = 0;
            }
        }

        if (lastNimbusTime == null || !reportedTime.equals(lastReportedTime)) {
            lastNimbusTime = Time.currentTimeSecs();
        }

        ret.put("is-timed-out", Time.deltaSecs(lastNimbusTime) >= timeout);
        ret.put("nimbus-time", lastNimbusTime);
        ret.put("executor-reported-time", reportedTime);
        ret.put(HEARTBEAT, newBeat);

        return ret;
    }


    /**
     * extracts a list of executor data from heart beats
     */
    public static List<Map<String, Object>> extractDataFromHb(Map executor2hostPort, Map task2component,
                                                              Map<List<Integer>, Map<String, Object>> beats,
                                                              boolean includeSys, StormTopology topology) {
        return extractDataFromHb(executor2hostPort, task2component, beats, includeSys, topology, null);
    }

    public static List<Map<String, Object>> extractDataFromHb(Map executor2hostPort, Map task2component,
                                                              Map<List<Integer>, Map<String, Object>> beats,
                                                              boolean includeSys, StormTopology topology, String compId) {
        List<Map<String, Object>> ret = new ArrayList<>();
        if (executor2hostPort == null || beats == null) {
            return ret;
        }
        for (Object o : executor2hostPort.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            List executor = (List) entry.getKey();
            List hostPort = (List) entry.getValue();

            Integer start = ((Number) executor.get(0)).intValue();
            Integer end = ((Number) executor.get(1)).intValue();

            String host = (String) hostPort.get(0);
            Integer port = ((Number) hostPort.get(1)).intValue();

            Map<String, Object> beat = beats.get(convertExecutor(executor));
            if (beat == null) {
                continue;
            }
            String id = (String) task2component.get(start);

            Map<String, Object> m = new HashMap<>();
            if ((compId == null || compId.equals(id)) && (includeSys || !Utils.isSystemId(id))) {
                putKV(m, "exec-id", entry.getKey());
                putKV(m, "comp-id", id);
                putKV(m, NUM_TASKS, end - start + 1);
                putKV(m, HOST, host);
                putKV(m, PORT, port);

                Map stats = getMapByKey(getMapByKey(beat, (HEARTBEAT)), STATS);
                putKV(m, UPTIME, getMapByKey(beat, HEARTBEAT).get(UPTIME));
                putKV(m, STATS, stats);

                String type = componentType(topology, compId);
                if (type != null) {
                    putKV(m, TYPE, type);
                } else {
                    putKV(m, TYPE, stats.get(TYPE));
                }
                ret.add(m);
            }
        }
        return ret;
    }

    /**
     * compute weighted avg from a Map of stats and given avg/count keys
     *
     * @param accData    a Map of {win -> key -> value}
     * @param wgtAvgKey  weighted average key
     * @param divisorKey count key
     * @return a Map of {win -> weighted avg value}
     */
    private static Map<String, Double> computeWeightedAveragesPerWindow(Map<String, Object> accData,
                                                                        String wgtAvgKey, String divisorKey) {
        Map<String, Double> ret = new HashMap<>();
        for (Object o : getMapByKey(accData, wgtAvgKey).entrySet()) {
            Map.Entry e = (Map.Entry) o;
            Object window = e.getKey();
            double wgtAvg = ((Number) e.getValue()).doubleValue();
            long divisor = ((Number) getMapByKey(accData, divisorKey).get(window)).longValue();
            if (divisor > 0) {
                ret.put(window.toString(), wgtAvg / divisor);
            }
        }
        return ret;
    }


    /**
     * convert a list of clojure executors to a java Set of List<Integer>
     */
    public static Set<List<Integer>> convertExecutors(Set executors) {
        Set<List<Integer>> convertedExecutors = new HashSet<>();
        for (Object executor : executors) {
            List l = (List) executor;
            convertedExecutors.add(convertExecutor(l));
        }
        return convertedExecutors;
    }

    /**
     * convert a clojure executor to java List<Integer>
     */
    public static List<Integer> convertExecutor(List executor) {
        return Lists.newArrayList(((Number) executor.get(0)).intValue(), ((Number) executor.get(1)).intValue());
    }

    /**
     * computes max bolt capacity
     *
     * @param executorSumms a list of ExecutorSummary
     * @return max bolt capacity
     */
    public static double computeBoltCapacity(List<ExecutorSummary> executorSumms) {
        double max = 0.0;
        for (ExecutorSummary summary : executorSumms) {
            double capacity = computeExecutorCapacity(summary);
            if (capacity > max) {
                max = capacity;
            }
        }
        return max;
    }

    public static double computeExecutorCapacity(ExecutorSummary summary) {
        ExecutorStats stats = summary.get_stats();
        if (stats == null) {
            return 0.0;
        } else {
            // actual value of m is: Map<String, Map<String/GlobalStreamId, Long/Double>> ({win -> stream -> value})
            Map<String, Map> m = aggregateBoltStats(Lists.newArrayList(summary), true);
            // {metric -> win -> value} ==> {win -> metric -> value}
            m = swapMapOrder(aggregateBoltStreams(m));
            // {metric -> value}
            Map data = getMapByKey(m, TEN_MIN_IN_SECONDS_STR);

            int uptime = summary.get_uptime_secs();
            int win = Math.min(uptime, TEN_MIN_IN_SECONDS);
            long executed = getByKeyOr0(data, EXECUTED).longValue();
            double latency = getByKeyOr0(data, EXEC_LATENCIES).doubleValue();
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
    public static List<ExecutorSummary> getFilledStats(List<ExecutorSummary> summs) {
        List<ExecutorSummary> ret = new ArrayList<>();
        for (ExecutorSummary summ : summs) {
            if (summ.get_stats() != null) {
                ret.add(summ);
            }
        }
        return ret;
    }

    private static <K, V> Map<String, V> mapKeyStr(Map<K, V> m) {
        Map<String, V> ret = new HashMap<>();
        for (Map.Entry<K, V> entry : m.entrySet()) {
            ret.put(entry.getKey().toString(), entry.getValue());
        }
        return ret;
    }

    private static <K1, K2> long sumStreamsLong(Map<K1, Map<K2, ?>> m, String key) {
        long sum = 0;
        if (m == null) {
            return sum;
        }
        for (Map<K2, ?> v : m.values()) {
            for (Map.Entry<K2, ?> entry : v.entrySet()) {
                if (entry.getKey().equals(key)) {
                    sum += ((Number) entry.getValue()).longValue();
                }
            }
        }
        return sum;
    }

    private static <K1, K2> double sumStreamsDouble(Map<K1, Map<K2, ?>> m, String key) {
        double sum = 0;
        if (m == null) {
            return sum;
        }
        for (Map<K2, ?> v : m.values()) {
            for (Map.Entry<K2, ?> entry : v.entrySet()) {
                if (entry.getKey().equals(key)) {
                    sum += ((Number) entry.getValue()).doubleValue();
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
    private static <K, V> Map<String, Map<K, V>> filterSysStreams(Map<String, Map<K, V>> stats, boolean includeSys) {
        if (!includeSys) {
            for (Iterator<String> itr = stats.keySet().iterator(); itr.hasNext(); ) {
                String winOrStream = itr.next();
                Map<K, V> stream2stat = stats.get(winOrStream);
                for (Iterator subItr = stream2stat.keySet().iterator(); subItr.hasNext(); ) {
                    Object key = subItr.next();
                    if (key instanceof String && Utils.isSystemId((String) key)) {
                        subItr.remove();
                    }
                }
            }
        }
        return stats;
    }

    /**
     * equals to clojure's: (merge-with (partial merge-with sum-or-0) acc-out spout-out)
     */
    private static <K1, K2> Map<K1, Map<K2, Number>> fullMergeWithSum(Map<K1, Map<K2, ?>> m1,
                                                                      Map<K1, Map<K2, ?>> m2) {
        Set<K1> allKeys = new HashSet<>();
        if (m1 != null) {
            allKeys.addAll(m1.keySet());
        }
        if (m2 != null) {
            allKeys.addAll(m2.keySet());
        }

        Map<K1, Map<K2, Number>> ret = new HashMap<>();
        for (K1 k : allKeys) {
            Map<K2, ?> mm1 = null, mm2 = null;
            if (m1 != null) {
                mm1 = m1.get(k);
            }
            if (m2 != null) {
                mm2 = m2.get(k);
            }
            ret.put(k, mergeWithSum(mm1, mm2));
        }

        return ret;
    }

    private static <K> Map<K, Number> mergeWithSum(Map<K, ?> m1, Map<K, ?> m2) {
        Map<K, Number> ret = new HashMap<>();

        Set<K> allKeys = new HashSet<>();
        if (m1 != null) {
            allKeys.addAll(m1.keySet());
        }
        if (m2 != null) {
            allKeys.addAll(m2.keySet());
        }

        for (K k : allKeys) {
            Number n1 = getOr0(m1, k);
            Number n2 = getOr0(m2, k);
            if (n1 instanceof Long) {
                ret.put(k, n1.longValue() + n2.longValue());
            } else {
                ret.put(k, n1.doubleValue() + n2.doubleValue());
            }
        }
        return ret;
    }

    private static <K> Map<K, Long> mergeWithSumLong(Map<K, Long> m1, Map<K, Long> m2) {
        Map<K, Long> ret = new HashMap<>();

        Set<K> allKeys = new HashSet<>();
        if (m1 != null) {
            allKeys.addAll(m1.keySet());
        }
        if (m2 != null) {
            allKeys.addAll(m2.keySet());
        }

        for (K k : allKeys) {
            Number n1 = getOr0(m1, k);
            Number n2 = getOr0(m2, k);
            ret.put(k, n1.longValue() + n2.longValue());
        }
        return ret;
    }

    private static <K> Map<K, Double> mergeWithSumDouble(Map<K, Double> m1, Map<K, Double> m2) {
        Map<K, Double> ret = new HashMap<>();

        Set<K> allKeys = new HashSet<>();
        if (m1 != null) {
            allKeys.addAll(m1.keySet());
        }
        if (m2 != null) {
            allKeys.addAll(m2.keySet());
        }

        for (K k : allKeys) {
            Number n1 = getOr0(m1, k);
            Number n2 = getOr0(m2, k);
            ret.put(k, n1.doubleValue() + n2.doubleValue());
        }
        return ret;
    }

    /**
     * this method merges 2 two-level-deep maps, which is different from mergeWithSum, and we expect the two maps
     * have the same keys
     */
    private static <K> Map<String, Map<K, List>> mergeWithAddPair(Map<String, Map<K, List>> m1,
                                                                  Map<String, Map<K, List>> m2) {
        Map<String, Map<K, List>> ret = new HashMap<>();

        Set<String> allKeys = new HashSet<>();
        if (m1 != null) {
            allKeys.addAll(m1.keySet());
        }
        if (m2 != null) {
            allKeys.addAll(m2.keySet());
        }

        for (String k : allKeys) {
            Map<K, List> mm1 = (m1 != null) ? m1.get(k) : null;
            Map<K, List> mm2 = (m2 != null) ? m2.get(k) : null;
            if (mm1 == null && mm2 == null) {
                continue;
            } else if (mm1 == null) {
                ret.put(k, mm2);
            } else if (mm2 == null) {
                ret.put(k, mm1);
            } else {
                Map<K, List> tmp = new HashMap<>();
                for (K kk : mm1.keySet()) {
                    List seq1 = mm1.get(kk);
                    List seq2 = mm2.get(kk);
                    List sums = new ArrayList();
                    for (int i = 0; i < seq1.size(); i++) {
                        if (seq1.get(i) instanceof Long) {
                            sums.add(((Number) seq1.get(i)).longValue() + ((Number) seq2.get(i)).longValue());
                        } else {
                            sums.add(((Number) seq1.get(i)).doubleValue() + ((Number) seq2.get(i)).doubleValue());
                        }
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

    public static ClusterWorkerHeartbeat thriftifyZkWorkerHb(Map<String, Object> heartbeat) {
        ClusterWorkerHeartbeat ret = new ClusterWorkerHeartbeat();
        ret.set_uptime_secs(getByKeyOr0(heartbeat, UPTIME).intValue());
        ret.set_storm_id((String) getByKey(heartbeat, "storm-id"));
        ret.set_time_secs(getByKeyOr0(heartbeat, TIME_SECS).intValue());

        // Map<List<Integer, Integer>, ExecutorStat>
        Map<ExecutorInfo, ExecutorStats> convertedStats = new HashMap<>();

        Map<List<Integer>, ExecutorStats> executorStats = getMapByKey(heartbeat, EXECUTOR_STATS);
        if (executorStats != null) {
            for (Map.Entry<List<Integer>, ExecutorStats> entry : executorStats.entrySet()) {
                List<Integer> executor = entry.getKey();
                ExecutorStats stats = entry.getValue();
                convertedStats.put(new ExecutorInfo(executor.get(0), executor.get(1)), stats);
            }
        }
        ret.set_executor_stats(convertedStats);

        return ret;
    }

    private static ComponentAggregateStats thriftifySpoutAggStats(Map m) {
        ComponentAggregateStats stats = new ComponentAggregateStats();
        stats.set_type(ComponentType.SPOUT);
        stats.set_last_error((ErrorInfo) getByKey(m, LAST_ERROR));
        thriftifyCommonAggStats(stats, m);

        SpoutAggregateStats spoutAggStats = new SpoutAggregateStats();
        spoutAggStats.set_complete_latency_ms(getByKeyOr0(m, COMP_LATENCY).doubleValue());
        SpecificAggregateStats specificStats = SpecificAggregateStats.spout(spoutAggStats);

        stats.set_specific_stats(specificStats);
        return stats;
    }

    private static ComponentAggregateStats thriftifyBoltAggStats(Map m) {
        ComponentAggregateStats stats = new ComponentAggregateStats();
        stats.set_type(ComponentType.BOLT);
        stats.set_last_error((ErrorInfo) getByKey(m, LAST_ERROR));
        thriftifyCommonAggStats(stats, m);

        BoltAggregateStats boltAggStats = new BoltAggregateStats();
        boltAggStats.set_execute_latency_ms(getByKeyOr0(m, EXEC_LATENCY).doubleValue());
        boltAggStats.set_process_latency_ms(getByKeyOr0(m, PROC_LATENCY).doubleValue());
        boltAggStats.set_executed(getByKeyOr0(m, EXECUTED).longValue());
        boltAggStats.set_capacity(getByKeyOr0(m, CAPACITY).doubleValue());
        SpecificAggregateStats specificStats = SpecificAggregateStats.bolt(boltAggStats);

        stats.set_specific_stats(specificStats);
        return stats;
    }

    private static ExecutorAggregateStats thriftifyExecAggStats(String compId, String compType, Map m) {
        ExecutorAggregateStats stats = new ExecutorAggregateStats();

        ExecutorSummary executorSummary = new ExecutorSummary();
        List executor = (List) getByKey(m, EXECUTOR_ID);
        executorSummary.set_executor_info(new ExecutorInfo(((Number) executor.get(0)).intValue(),
                ((Number) executor.get(1)).intValue()));
        executorSummary.set_component_id(compId);
        executorSummary.set_host((String) getByKey(m, HOST));
        executorSummary.set_port(getByKeyOr0(m, PORT).intValue());
        int uptime = getByKeyOr0(m, UPTIME).intValue();
        executorSummary.set_uptime_secs(uptime);
        stats.set_exec_summary(executorSummary);

        if (compType.equals(SPOUT)) {
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
        commonStats.set_num_tasks(getByKeyOr0(m, NUM_TASKS).intValue());
        commonStats.set_num_executors(getByKeyOr0(m, NUM_EXECUTORS).intValue());
        commonStats.set_emitted(getByKeyOr0(m, EMITTED).longValue());
        commonStats.set_transferred(getByKeyOr0(m, TRANSFERRED).longValue());
        commonStats.set_acked(getByKeyOr0(m, ACKED).longValue());
        commonStats.set_failed(getByKeyOr0(m, FAILED).longValue());

        stats.set_common_stats(commonStats);
        return stats;
    }

    private static ComponentPageInfo thriftifyCompPageData(
            String topologyId, StormTopology topology, String compId, Map<String, Object> data) {
        ComponentPageInfo ret = new ComponentPageInfo();
        ret.set_component_id(compId);

        Map win2stats = new HashMap();
        putKV(win2stats, EMITTED, getMapByKey(data, WIN_TO_EMITTED));
        putKV(win2stats, TRANSFERRED, getMapByKey(data, WIN_TO_TRANSFERRED));
        putKV(win2stats, ACKED, getMapByKey(data, WIN_TO_ACKED));
        putKV(win2stats, FAILED, getMapByKey(data, WIN_TO_FAILED));

        String compType = (String) data.get(TYPE);
        if (compType.equals(SPOUT)) {
            ret.set_component_type(ComponentType.SPOUT);
            putKV(win2stats, COMP_LATENCY, getMapByKey(data, WIN_TO_COMP_LAT));
        } else {
            ret.set_component_type(ComponentType.BOLT);
            putKV(win2stats, EXEC_LATENCY, getMapByKey(data, WIN_TO_EXEC_LAT));
            putKV(win2stats, PROC_LATENCY, getMapByKey(data, WIN_TO_PROC_LAT));
            putKV(win2stats, EXECUTED, getMapByKey(data, WIN_TO_EXECUTED));
        }
        win2stats = swapMapOrder(win2stats);

        List<ExecutorAggregateStats> execStats = new ArrayList<>();
        List executorStats = (List) getByKey(data, EXECUTOR_STATS);
        if (executorStats != null) {
            for (Object o : executorStats) {
                execStats.add(thriftifyExecAggStats(compId, compType, (Map) o));
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
            sid2outputStats = thriftifySpoutOutputStats(getMapByKey(data, SID_TO_OUT_STATS));
        } else {
            Map tmp = new HashMap();
            for (Object k : win2stats.keySet()) {
                tmp.put(k, thriftifyBoltAggStats((Map) win2stats.get(k)));
            }
            win2stats = tmp;
            gsid2inputStats = thriftifyBoltInputStats(getMapByKey(data, CID_SID_TO_IN_STATS));
            sid2outputStats = thriftifyBoltOutputStats(getMapByKey(data, SID_TO_OUT_STATS));
        }
        ret.set_num_executors(getByKeyOr0(data, NUM_EXECUTORS).intValue());
        ret.set_num_tasks(getByKeyOr0(data, NUM_TASKS).intValue());
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

        ret.set_emitted(windowSetConverter(getMapByKey(stats, EMITTED), TO_STRING, TO_STRING));
        ret.set_transferred(windowSetConverter(getMapByKey(stats, TRANSFERRED), TO_STRING, TO_STRING));
        ret.set_rate(((Number) getByKey(stats, RATE)).doubleValue());

        return ret;
    }

    private static ExecutorSpecificStats thriftifySpecificStats(Map stats) {
        ExecutorSpecificStats specificStats = new ExecutorSpecificStats();

        String compType = (String) getByKey(stats, TYPE);
        if (BOLT.equals(compType)) {
            BoltStats boltStats = new BoltStats();
            boltStats.set_acked(windowSetConverter(getMapByKey(stats, ACKED), TO_GSID, TO_STRING));
            boltStats.set_executed(windowSetConverter(getMapByKey(stats, EXECUTED), TO_GSID, TO_STRING));
            boltStats.set_execute_ms_avg(windowSetConverter(getMapByKey(stats, EXEC_LATENCIES), TO_GSID, TO_STRING));
            boltStats.set_failed(windowSetConverter(getMapByKey(stats, FAILED), TO_GSID, TO_STRING));
            boltStats.set_process_ms_avg(windowSetConverter(getMapByKey(stats, PROC_LATENCIES), TO_GSID, TO_STRING));
            specificStats.set_bolt(boltStats);
        } else {
            SpoutStats spoutStats = new SpoutStats();
            spoutStats.set_acked(windowSetConverter(getMapByKey(stats, ACKED), TO_STRING, TO_STRING));
            spoutStats.set_failed(windowSetConverter(getMapByKey(stats, FAILED), TO_STRING, TO_STRING));
            spoutStats.set_complete_ms_avg(windowSetConverter(getMapByKey(stats, COMP_LATENCIES), TO_STRING, TO_STRING));
            specificStats.set_spout(spoutStats);
        }
        return specificStats;
    }


    // =====================================================================================
    // helper methods
    // =====================================================================================

    public static Map<List<Integer>, ExecutorStats> mkEmptyExecutorZkHbs(Set executors) {
        Map<List<Integer>, ExecutorStats> ret = new HashMap<>();
        for (Object executor : executors) {
            List startEnd = (List) executor;
            ret.put(convertExecutor(startEnd), null);
        }
        return ret;
    }

    /**
     * convert clojure structure to java maps
     */
    public static Map<List<Integer>, ExecutorStats> convertExecutorZkHbs(Map executorBeats) {
        Map<List<Integer>, ExecutorStats> ret = new HashMap<>();
        for (Object executorBeat : executorBeats.entrySet()) {
            Map.Entry entry = (Map.Entry) executorBeat;
            List startEnd = (List) entry.getKey();
            ret.put(convertExecutor(startEnd), (ExecutorStats) entry.getValue());
        }
        return ret;
    }

    public static Map<String, Object> mkZkWorkerHb(String stormId, Map<List<Integer>, ExecutorStats> executorStats, Integer uptime) {
        Map<String, Object> ret = new HashMap<>();
        ret.put("storm-id", stormId);
        ret.put(EXECUTOR_STATS, executorStats);
        ret.put(UPTIME, uptime);
        ret.put(TIME_SECS, Time.currentTimeSecs());

        return ret;
    }

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
            Map execAvg = (Map) ((Map) getByKey(m, EXEC_LATENCIES)).get(TEN_MIN_IN_SECONDS_STR);
            Map exec = (Map) ((Map) getByKey(m, EXECUTED)).get(TEN_MIN_IN_SECONDS_STR);

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

    private static Number getByKeyOr0(Map m, String k) {
        if (m == null) {
            return 0;
        }

        Number n = (Number) m.get(k);
        if (n == null) {
            return 0;
        }
        return n;
    }

    private static <T, V1 extends Number, V2 extends Number> Double weightAvgAndSum(Map<T, V1> id2Avg, Map<T, V2> id2num) {
        double ret = 0;
        if (id2Avg == null || id2num == null) {
            return ret;
        }

        for (Map.Entry<T, V1> entry : id2Avg.entrySet()) {
            T k = entry.getKey();
            ret += productOr0(entry.getValue(), id2num.get(k));
        }
        return ret;
    }

    private static <K, V1 extends Number, V2 extends Number> double weightAvg(Map<K, V1> id2Avg, Map<K, V2> id2num, K key) {
        if (id2Avg == null || id2num == null) {
            return 0.0;
        }
        return productOr0(id2Avg.get(key), id2num.get(key));
    }

    public static String componentType(StormTopology topology, String compId) {
        if (compId == null) {
            return null;
        }

        Map<String, Bolt> bolts = topology.get_bolts();
        if (Utils.isSystemId(compId) || bolts.containsKey(compId)) {
            return BOLT;
        }
        return SPOUT;
    }

    public static void putKV(Map map, String k, Object v) {
        map.put(k, v);
    }

    private static void remove(Map map, String k) {
        map.remove(k);
    }

    public static Object getByKey(Map map, String key) {
        return map.get(key);
    }

    public static Map getMapByKey(Map map, String key) {
        if (map == null) {
            return null;
        }
        return (Map) map.get(key);
    }

    private static <K, V extends Number> long sumValues(Map<K, V> m) {
        long ret = 0L;
        if (m == null) {
            return ret;
        }

        for (Number n : m.values()) {
            ret += n.longValue();
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
     * @param avgs   a HashMap of values: { win -> GlobalStreamId -> value }
     * @param counts a HashMap of values: { win -> GlobalStreamId -> value }
     * @return a HashMap of values: {win -> GlobalStreamId -> [cnt*avg, cnt]}
     */
    private static <K> Map<String, Map<K, List>> expandAverages(Map<String, Map<K, Double>> avgs,
                                                                Map<String, Map<K, Long>> counts) {
        Map<String, Map<K, List>> ret = new HashMap<>();

        for (String win : counts.keySet()) {
            Map<K, List> inner = new HashMap<>();

            Map<K, Long> stream2cnt = counts.get(win);
            for (K stream : stream2cnt.keySet()) {
                Long cnt = stream2cnt.get(stream);
                Double avg = avgs.get(win).get(stream);
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
    private static <K> Map<String, Map<K, List>> expandAveragesSeq(
            List<Map<String, Map<K, Double>>> avgSeq, List<Map<String, Map<K, Long>>> countSeq) {
        Map<String, Map<K, List>> initVal = null;
        for (int i = 0; i < avgSeq.size(); i++) {
            Map<String, Map<K, Double>> avg = avgSeq.get(i);
            Map<String, Map<K, Long>> count = (Map) countSeq.get(i);
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

    private static ErrorInfo getLastError(IStormClusterState stormClusterState, String stormId, String compId) {
        return stormClusterState.lastError(stormId, compId);
    }


    // =====================================================================================
    // key transformers
    // =====================================================================================

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

    static class FromGlobalStreamIdTransformer implements KeyTransformer<List<String>> {
        @Override
        public List<String> transform(Object key) {
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
