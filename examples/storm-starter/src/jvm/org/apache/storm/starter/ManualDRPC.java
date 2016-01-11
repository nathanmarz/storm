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
import org.apache.storm.LocalDRPC;
import org.apache.storm.drpc.DRPCSpout;
import org.apache.storm.drpc.ReturnResults;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


public class ManualDRPC {
  public static class ExclamationBolt extends BaseBasicBolt {

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("result", "return-info"));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String arg = tuple.getString(0);
      Object retInfo = tuple.getValue(1);
      collector.emit(new Values(arg + "!!!", retInfo));
    }

  }

  public static void main(String[] args) {
    TopologyBuilder builder = new TopologyBuilder();
    LocalDRPC drpc = new LocalDRPC();

    DRPCSpout spout = new DRPCSpout("exclamation", drpc);
    builder.setSpout("drpc", spout);
    builder.setBolt("exclaim", new ExclamationBolt(), 3).shuffleGrouping("drpc");
    builder.setBolt("return", new ReturnResults(), 3).shuffleGrouping("exclaim");

    LocalCluster cluster = new LocalCluster();
    Config conf = new Config();
    cluster.submitTopology("exclaim", conf, builder.createTopology());

    System.out.println(drpc.execute("exclamation", "aaa"));
    System.out.println(drpc.execute("exclamation", "bbb"));

  }
}
