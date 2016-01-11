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

import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;

import org.apache.storm.eventhubs.samples.TransactionalTridentEventCount.LoggingFilter;
import org.apache.storm.eventhubs.spout.EventHubSpout;
import org.apache.storm.eventhubs.trident.OpaqueTridentEventHubSpout;

/**
 * A simple Trident topology uses OpaqueTridentEventHubSpout
 */
public class OpaqueTridentEventCount extends EventCount {
  @Override
  protected StormTopology buildTopology(EventHubSpout eventHubSpout) {
    TridentTopology topology = new TridentTopology();
    
    OpaqueTridentEventHubSpout spout = new OpaqueTridentEventHubSpout(spoutConfig);
    TridentState state = topology.newStream("stream-" + spoutConfig.getTopologyName(), spout)
        .parallelismHint(spoutConfig.getPartitionCount())
        .aggregate(new Count(), new Fields("partial-count"))
        .persistentAggregate(new MemoryMapState.Factory(), new Fields("partial-count"), new Sum(), new Fields("count"));
    state.newValuesStream().each(new Fields("count"), new LoggingFilter("got count: ", 10000));
    return topology.build();
  }
  
  public static void main(String[] args) throws Exception {
    OpaqueTridentEventCount scenario = new OpaqueTridentEventCount();
    scenario.runScenario(args);
  }
}
