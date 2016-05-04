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
package org.apache.storm.eventhubs.trident;

import java.util.Map;

import org.apache.storm.eventhubs.spout.EventHubSpoutConfig;
import org.apache.storm.eventhubs.spout.IEventDataScheme;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.trident.spout.IPartitionedTridentSpout;
import org.apache.storm.eventhubs.trident.Partition;

/**
 * Transactional Trident EventHub Spout
 */
public class TransactionalTridentEventHubSpout implements 
  IPartitionedTridentSpout<Partitions, Partition, Map> {
  private static final long serialVersionUID = 1L;
  private final IEventDataScheme scheme;
  private final EventHubSpoutConfig spoutConfig;
  
  public TransactionalTridentEventHubSpout(EventHubSpoutConfig config) {
    spoutConfig = config;
    scheme = spoutConfig.getEventDataScheme();
  }
  
  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }

  @Override
  public IPartitionedTridentSpout.Coordinator<Partitions> getCoordinator(
      Map conf, TopologyContext context) {
    return new org.apache.storm.eventhubs.trident.Coordinator(spoutConfig);
  }

  @Override
  public IPartitionedTridentSpout.Emitter<Partitions, Partition, Map> getEmitter(
      Map conf, TopologyContext context) {
    return new TransactionalTridentEventHubEmitter(spoutConfig);
  }

  @Override
  public Fields getOutputFields() {
    return scheme.getOutputFields();
  }

}
