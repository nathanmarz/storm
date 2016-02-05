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
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.SpoutDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

public class ResourceAwareExampleTopology {
  public static class ExclamationBolt extends BaseRichBolt {
    OutputCollector _collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
      _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
      _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }
  }

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();

    SpoutDeclarer spout =  builder.setSpout("word", new TestWordSpout(), 10);
    //set cpu requirement
    spout.setCPULoad(20);
    //set onheap and offheap memory requirement
    spout.setMemoryLoad(64, 16);

    BoltDeclarer bolt1 = builder.setBolt("exclaim1", new ExclamationBolt(), 3).shuffleGrouping("word");
    //sets cpu requirement.  Not neccessary to set both CPU and memory.
    //For requirements not set, a default value will be used
    bolt1.setCPULoad(15);

    BoltDeclarer bolt2 = builder.setBolt("exclaim2", new ExclamationBolt(), 2).shuffleGrouping("exclaim1");
    bolt2.setMemoryLoad(100);

    Config conf = new Config();
    conf.setDebug(true);

    /**
     * Use to limit the maximum amount of memory (in MB) allocated to one worker process.
     * Can be used to spread executors to to multiple workers
     */
    conf.setTopologyWorkerMaxHeapSize(1024.0);

    //topology priority describing the importance of the topology in decreasing importance starting from 0 (i.e. 0 is the highest priority and the priority importance decreases as the priority number increases).
    //Recommended range of 0-29 but no hard limit set.
    conf.setTopologyPriority(29);

    // Set strategy to schedule topology. If not specified, default to org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy
    conf.setTopologyStrategy(org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy.class);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
    else {

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("test", conf, builder.createTopology());
      Utils.sleep(10000);
      cluster.killTopology("test");
      cluster.shutdown();
    }
  }
}
