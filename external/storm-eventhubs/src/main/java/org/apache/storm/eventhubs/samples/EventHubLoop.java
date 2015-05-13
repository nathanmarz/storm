/*******************************************************************************
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
 *******************************************************************************/
package org.apache.storm.eventhubs.samples;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

import org.apache.storm.eventhubs.bolt.EventHubBolt;
import org.apache.storm.eventhubs.bolt.EventHubBoltConfig;
import org.apache.storm.eventhubs.spout.EventHubSpout;

/**
 * A sample topology that loops message back to EventHub
 */
public class EventHubLoop extends EventCount {

  @Override
  protected StormTopology buildTopology(EventHubSpout eventHubSpout) {
    TopologyBuilder topologyBuilder = new TopologyBuilder();

    topologyBuilder.setSpout("EventHubsSpout", eventHubSpout, spoutConfig.getPartitionCount())
      .setNumTasks(spoutConfig.getPartitionCount());
    EventHubBoltConfig boltConfig = new EventHubBoltConfig(spoutConfig.getConnectionString(),
        spoutConfig.getEntityPath(), true);
    
    EventHubBolt eventHubBolt = new EventHubBolt(boltConfig);
    int boltTasks = spoutConfig.getPartitionCount();
    topologyBuilder.setBolt("EventHubsBolt", eventHubBolt, boltTasks)
      .localOrShuffleGrouping("EventHubsSpout").setNumTasks(boltTasks);
    return topologyBuilder.createTopology();
  }
  
  public static void main(String[] args) throws Exception {
    EventHubLoop scenario = new EventHubLoop();
    scenario.runScenario(args);
  }
}
