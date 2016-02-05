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
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.*;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.FailedException;
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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class InOrderDeliveryTest {
  public static class InOrderSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    int _base = 0;
    int _i = 0;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
      _collector = collector;
      _base = context.getThisTaskIndex();
    }

    @Override
    public void nextTuple() {
      Values v = new Values(_base, _i);
      _collector.emit(v, "ACK");
      _i++;
    }

    @Override
    public void ack(Object id) {
      //Ignored
    }

    @Override
    public void fail(Object id) {
      //Ignored
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("c1", "c2"));
    }
  }

  public static class Check extends BaseBasicBolt {
    Map<Integer, Integer> expected = new HashMap<Integer, Integer>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      Integer c1 = tuple.getInteger(0);
      Integer c2 = tuple.getInteger(1);
      Integer exp = expected.get(c1);
      if (exp == null) exp = 0;
      if (c2.intValue() != exp.intValue()) {
          System.out.println(c1+" "+c2+" != "+exp);
          throw new FailedException(c1+" "+c2+" != "+exp);
      }
      exp = c2 + 1;
      expected.put(c1, exp);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      //Empty
    }
  }

  public static void printMetrics(Nimbus.Client client, String name) throws Exception {
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
    double weightedAvgTotal = 0.0;
    for (ExecutorSummary exec: info.get_executors()) {
      if ("spout".equals(exec.get_component_id())) {
        SpoutStats stats = exec.get_stats().get_specific().get_spout();
        Map<String, Long> failedMap = stats.get_failed().get(":all-time");
        Map<String, Long> ackedMap = stats.get_acked().get(":all-time");
        Map<String, Double> avgLatMap = stats.get_complete_ms_avg().get(":all-time");
        for (String key: ackedMap.keySet()) {
          if (failedMap != null) {
              Long tmp = failedMap.get(key);
              if (tmp != null) {
                  failed += tmp;
              }
          }
          long ackVal = ackedMap.get(key);
          double latVal = avgLatMap.get(key) * ackVal;
          acked += ackVal;
          weightedAvgTotal += latVal;
        }
      }
    }
    double avgLatency = weightedAvgTotal/acked;
    System.out.println("uptime: "+uptime+" acked: "+acked+" avgLatency: "+avgLatency+" acked/sec: "+(((double)acked)/uptime+" failed: "+failed));
  } 

  public static void kill(Nimbus.Client client, String name) throws Exception {
    KillOptions opts = new KillOptions();
    opts.set_wait_secs(0);
    client.killTopologyWithOpts(name, opts);
  } 

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", new InOrderSpout(), 8);
    builder.setBolt("count", new Check(), 8).fieldsGrouping("spout", new Fields("c1"));

    Config conf = new Config();
    conf.registerMetricsConsumer(org.apache.storm.metric.LoggingMetricsConsumer.class);

    String name = "in-order-test";
    if (args != null && args.length > 0) {
        name = args[0];
    }

    conf.setNumWorkers(1);
    StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());

    Map clusterConf = Utils.readStormConfig();
    clusterConf.putAll(Utils.readCommandLineOpts());
    Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();

    //Sleep for 50 mins
    for (int i = 0; i < 50; i++) {
        Thread.sleep(30 * 1000);
        printMetrics(client, name);
    }
    kill(client, name);
  }
}
