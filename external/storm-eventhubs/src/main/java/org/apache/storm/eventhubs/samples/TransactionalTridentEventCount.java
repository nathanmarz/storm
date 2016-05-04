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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;

import org.apache.storm.eventhubs.spout.EventHubSpout;
import org.apache.storm.eventhubs.trident.TransactionalTridentEventHubSpout;

import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * A simple Trident topology uses TransactionalTridentEventHubSpout
 */
public class TransactionalTridentEventCount extends EventCount {
  public static class LoggingFilter extends BaseFilter {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(LoggingFilter.class);
    private final String prefix;
    private final long logIntervalMs;
    private long lastTime;
    public LoggingFilter(String prefix, int logIntervalMs) {
      this.prefix = prefix;
      this.logIntervalMs = logIntervalMs;
      lastTime = System.nanoTime();
    }

    @Override
    public boolean isKeep(TridentTuple tuple) {
      long now = System.nanoTime();
      if(logIntervalMs < (now - lastTime) / 1000000) {
        logger.info(prefix + tuple.toString());
        lastTime = now;
      }
      return false;
    }
  }
  
  @Override
  protected StormTopology buildTopology(EventHubSpout eventHubSpout) {
    TridentTopology topology = new TridentTopology();
    
    TransactionalTridentEventHubSpout spout = new TransactionalTridentEventHubSpout(spoutConfig);
    TridentState state = topology.newStream("stream-" + spoutConfig.getTopologyName(), spout)
        .parallelismHint(spoutConfig.getPartitionCount())
        .aggregate(new Count(), new Fields("partial-count"))
        .persistentAggregate(new MemoryMapState.Factory(), new Fields("partial-count"), new Sum(), new Fields("count"));
    state.newValuesStream().each(new Fields("count"), new LoggingFilter("got count: ", 10000));
    return topology.build();
  }
  
  public static void main(String[] args) throws Exception {
    TransactionalTridentEventCount scenario = new TransactionalTridentEventCount();
    scenario.runScenario(args);
  }
}
