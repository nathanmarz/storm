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
package org.apache.storm.starter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.metric.HttpForwardingMetricsServer;
import org.apache.storm.metric.HttpForwardingMetricsConsumer;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.metric.api.IMetricsConsumer.TaskInfo;
import org.apache.storm.metric.api.IMetricsConsumer.DataPoint;
import org.apache.storm.generated.*;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.apache.storm.StormSubmitter;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.storm.metrics.hdrhistogram.HistogramMetric;
import org.HdrHistogram.Histogram;

/**
 * WordCount but the spout goes at a predefined rate and we collect
 * proper latency statistics.
 */
public class ThroughputVsLatency {
  private static class SentWithTime {
    public final String sentence;
    public final long time;

    SentWithTime(String sentence, long time) {
        this.sentence = sentence;
        this.time = time;
    }
  }

  public static class C {
    LocalCluster _local = null;
    Nimbus.Client _client = null;

    public C(Map conf) {
      Map clusterConf = Utils.readStormConfig();
      if (conf != null) {
        clusterConf.putAll(conf);
      }
      Boolean isLocal = (Boolean)clusterConf.get("run.local");
      if (isLocal != null && isLocal) {
        _local = new LocalCluster();
      } else {
        _client = NimbusClient.getConfiguredClient(clusterConf).getClient();
      }
    }

    public ClusterSummary getClusterInfo() throws Exception {
      if (_local != null) {
        return _local.getClusterInfo();
      } else {
        return _client.getClusterInfo();
      }
    }

    public TopologyInfo getTopologyInfo(String id) throws Exception {
      if (_local != null) {
        return _local.getTopologyInfo(id);
      } else {
        return _client.getTopologyInfo(id);
      }
    }

    public void killTopologyWithOpts(String name, KillOptions opts) throws Exception {
      if (_local != null) {
        _local.killTopologyWithOpts(name, opts);
      } else {
        _client.killTopologyWithOpts(name, opts);
      }
    }

    public void submitTopology(String name, Map stormConf, StormTopology topology) throws Exception {
      if (_local != null) {
        _local.submitTopology(name, stormConf, topology);
      } else {
        StormSubmitter.submitTopology(name, stormConf, topology);
      }
    }

    public boolean isLocal() {
      return _local != null;
    }
  }

  public static class FastRandomSentenceSpout extends BaseRichSpout {
    static final String[] SENTENCES = new String[]{ "the cow jumped over the moon", "an apple a day keeps the doctor away",
          "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature" };

    SpoutOutputCollector _collector;
    long _periodNano;
    long _emitAmount;
    Random _rand;
    long _nextEmitTime;
    long _emitsLeft;
    HistogramMetric _histo;

    public FastRandomSentenceSpout(long ratePerSecond) {
        if (ratePerSecond > 0) {
            _periodNano = Math.max(1, 1000000000/ratePerSecond);
            _emitAmount = Math.max(1, (long)((ratePerSecond / 1000000000.0) * _periodNano));
        } else {
            _periodNano = Long.MAX_VALUE - 1;
            _emitAmount = 1;
        }
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
      _collector = collector;
      _rand = ThreadLocalRandom.current();
      _nextEmitTime = System.nanoTime();
      _emitsLeft = _emitAmount;
      _histo = new HistogramMetric(3600000000000L, 3);
      context.registerMetric("comp-lat-histo", _histo, 10); //Update every 10 seconds, so we are not too far behind
    }

    @Override
    public void nextTuple() {
      if (_emitsLeft <= 0 && _nextEmitTime <= System.nanoTime()) {
          _emitsLeft = _emitAmount;
          _nextEmitTime = _nextEmitTime + _periodNano;
      }

      if (_emitsLeft > 0) {
          String sentence = SENTENCES[_rand.nextInt(SENTENCES.length)];
          _collector.emit(new Values(sentence), new SentWithTime(sentence, _nextEmitTime - _periodNano));
          _emitsLeft--;
      }
    }

    @Override
    public void ack(Object id) {
      long end = System.nanoTime();
      SentWithTime st = (SentWithTime)id;
      _histo.recordValue(end-st.time);
    }

    @Override
    public void fail(Object id) {
      SentWithTime st = (SentWithTime)id;
      _collector.emit(new Values(st.sentence), id);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("sentence"));
    }
  }

  public static class SplitSentence extends BaseBasicBolt {
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String sentence = tuple.getString(0);
      for (String word: sentence.split("\\s+")) {
          collector.emit(new Values(word, 1));
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }
  }

  public static class WordCount extends BaseBasicBolt {
    Map<String, Integer> counts = new HashMap<String, Integer>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String word = tuple.getString(0);
      Integer count = counts.get(word);
      if (count == null)
        count = 0;
      count++;
      counts.put(word, count);
      collector.emit(new Values(word, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }
  }

  private static class MemMeasure {
    private long _mem = 0;
    private long _time = 0;

    public synchronized void update(long mem) {
        _mem = mem;
        _time = System.currentTimeMillis();
    }

    public synchronized long get() {
        return isExpired() ? 0l : _mem;
    }

    public synchronized boolean isExpired() {
        return (System.currentTimeMillis() - _time) >= 20000;
    }
  }

  private static final Histogram _histo = new Histogram(3600000000000L, 3);
  private static final AtomicLong _systemCPU = new AtomicLong(0);
  private static final AtomicLong _userCPU = new AtomicLong(0);
  private static final AtomicLong _gcCount = new AtomicLong(0);
  private static final AtomicLong _gcMs = new AtomicLong(0);
  private static final ConcurrentHashMap<String, MemMeasure> _memoryBytes = new ConcurrentHashMap<String, MemMeasure>();

  private static long readMemory() {
    long total = 0;
    for (MemMeasure mem: _memoryBytes.values()) {
      total += mem.get();
    }
    return total;
  }

  private static long _prev_acked = 0;
  private static long _prev_uptime = 0;

  public static void printMetrics(C client, String name) throws Exception {
    ClusterSummary summary = client.getClusterInfo();
    String id = null;
    for (TopologySummary ts: summary.get_topologies()) {
      if (name.equals(ts.get_name())) {
        id = ts.get_id();
      }
    }
    if (id == null) {
      throw new Exception("Could not find a topology named "+name);
    }
    TopologyInfo info = client.getTopologyInfo(id);
    int uptime = info.get_uptime_secs();
    long acked = 0;
    long failed = 0;
    for (ExecutorSummary exec: info.get_executors()) {
      if ("spout".equals(exec.get_component_id()) && exec.get_stats() != null && exec.get_stats().get_specific() != null) {
        SpoutStats stats = exec.get_stats().get_specific().get_spout();
        Map<String, Long> failedMap = stats.get_failed().get(":all-time");
        Map<String, Long> ackedMap = stats.get_acked().get(":all-time");
        if (ackedMap != null) {
          for (String key: ackedMap.keySet()) {
            if (failedMap != null) {
              Long tmp = failedMap.get(key);
              if (tmp != null) {
                  failed += tmp;
              }
            }
            long ackVal = ackedMap.get(key);
            acked += ackVal;
          }
        }
      }
    }
    long ackedThisTime = acked - _prev_acked;
    long thisTime = uptime - _prev_uptime;
    long nnpct, nnnpct, min, max;
    double mean, stddev;
    synchronized(_histo) {
      nnpct = _histo.getValueAtPercentile(99.0);
      nnnpct = _histo.getValueAtPercentile(99.9);
      min = _histo.getMinValue();
      max = _histo.getMaxValue();
      mean = _histo.getMean();
      stddev = _histo.getStdDeviation();
      _histo.reset();
    }
    long user = _userCPU.getAndSet(0);
    long sys = _systemCPU.getAndSet(0);
    long gc = _gcMs.getAndSet(0);
    double memMB = readMemory() / (1024.0 * 1024.0);
    System.out.printf("uptime: %,4d acked: %,9d acked/sec: %,10.2f failed: %,8d " +
                      "99%%: %,15d 99.9%%: %,15d min: %,15d max: %,15d mean: %,15.2f " +
                      "stddev: %,15.2f user: %,10d sys: %,10d gc: %,10d mem: %,10.2f\n",
                       uptime, ackedThisTime, (((double)ackedThisTime)/thisTime), failed, nnpct, nnnpct,
                       min, max, mean, stddev, user, sys, gc, memMB);
    _prev_uptime = uptime;
    _prev_acked = acked;
  }

  public static void kill(C client, String name) throws Exception {
    KillOptions opts = new KillOptions();
    opts.set_wait_secs(0);
    client.killTopologyWithOpts(name, opts);
  }

  public static void main(String[] args) throws Exception {
    long ratePerSecond = 500;
    if (args != null && args.length > 0) {
        ratePerSecond = Long.valueOf(args[0]);
    }

    int parallelism = 4;
    if (args != null && args.length > 1) {
        parallelism = Integer.valueOf(args[1]);
    }

    int numMins = 5;
    if (args != null && args.length > 2) {
        numMins = Integer.valueOf(args[2]);
    }

    String name = "wc-test";
    if (args != null && args.length > 3) {
        name = args[3];
    }

    Config conf = new Config();
    HttpForwardingMetricsServer metricServer = new HttpForwardingMetricsServer(conf) {
        @Override
        public void handle(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
            String worker = taskInfo.srcWorkerHost + ":" + taskInfo.srcWorkerPort;
            for (DataPoint dp: dataPoints) {
                if ("comp-lat-histo".equals(dp.name) && dp.value instanceof Histogram) {
                    synchronized(_histo) {
                        _histo.add((Histogram)dp.value);
                    }
                } else if ("CPU".equals(dp.name) && dp.value instanceof Map) {
                   Map<Object, Object> m = (Map<Object, Object>)dp.value;
                   Object sys = m.get("sys-ms");
                   if (sys instanceof Number) {
                       _systemCPU.getAndAdd(((Number)sys).longValue());
                   }
                   Object user = m.get("user-ms");
                   if (user instanceof Number) {
                       _userCPU.getAndAdd(((Number)user).longValue());
                   }
                } else if (dp.name.startsWith("GC/") && dp.value instanceof Map) {
                   Map<Object, Object> m = (Map<Object, Object>)dp.value;
                   Object count = m.get("count");
                   if (count instanceof Number) {
                       _gcCount.getAndAdd(((Number)count).longValue());
                   }
                   Object time = m.get("timeMs");
                   if (time instanceof Number) {
                       _gcMs.getAndAdd(((Number)time).longValue());
                   }
                } else if (dp.name.startsWith("memory/") && dp.value instanceof Map) {
                   Map<Object, Object> m = (Map<Object, Object>)dp.value;
                   Object val = m.get("usedBytes");
                   if (val instanceof Number) {
                       MemMeasure mm = _memoryBytes.get(worker);
                       if (mm == null) {
                         mm = new MemMeasure();
                         MemMeasure tmp = _memoryBytes.putIfAbsent(worker, mm);
                         mm = tmp == null ? mm : tmp; 
                       }
                       mm.update(((Number)val).longValue());
                   }
                }
            }
        }
    };

    metricServer.serve();
    String url = metricServer.getUrl();

    C cluster = new C(conf);
    conf.setNumWorkers(parallelism);
    conf.registerMetricsConsumer(org.apache.storm.metric.LoggingMetricsConsumer.class);
    conf.registerMetricsConsumer(org.apache.storm.metric.HttpForwardingMetricsConsumer.class, url, 1);
    Map<String, String> workerMetrics = new HashMap<String, String>();
    if (!cluster.isLocal()) {
      //sigar uses JNI and does not work in local mode
      workerMetrics.put("CPU", "org.apache.storm.metrics.sigar.CPUMetric");
    }
    conf.put(Config.TOPOLOGY_WORKER_METRICS, workerMetrics);
    conf.put(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS, 10);
    conf.put(Config.TOPOLOGY_WORKER_GC_CHILDOPTS,
      "-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:NewSize=128m -XX:CMSInitiatingOccupancyFraction=70 -XX:-CMSConcurrentMTEnabled");
    conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-Xmx2g");

    TopologyBuilder builder = new TopologyBuilder();

    int numEach = 4 * parallelism;
    builder.setSpout("spout", new FastRandomSentenceSpout(ratePerSecond/numEach), numEach);

    builder.setBolt("split", new SplitSentence(), numEach).shuffleGrouping("spout");
    builder.setBolt("count", new WordCount(), numEach).fieldsGrouping("split", new Fields("word"));

    try {
        cluster.submitTopology(name, conf, builder.createTopology());

        for (int i = 0; i < numMins * 2; i++) {
            Thread.sleep(30 * 1000);
            printMetrics(cluster, name);
        }
    } finally {
        kill(cluster, name);
    }
    System.exit(0);
  }
}
