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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.trident.spout.IOpaquePartitionedTridentSpout;
import org.apache.storm.trident.spout.IPartitionedTridentSpout;

import org.apache.storm.eventhubs.spout.EventHubSpoutConfig;

public class Coordinator implements IPartitionedTridentSpout.Coordinator<Partitions>,
    IOpaquePartitionedTridentSpout.Coordinator<Partitions> {
  private static final Logger logger = LoggerFactory.getLogger(Coordinator.class);
  private final EventHubSpoutConfig spoutConfig;
  Partitions partitions;
  
  public Coordinator(EventHubSpoutConfig spoutConfig) {
    this.spoutConfig = spoutConfig;
  }

  @Override
  public void close() {
  }

  @Override
  public Partitions getPartitionsForBatch() {
    if(partitions != null) {
      return partitions;
    }
    
    partitions = new Partitions();
    for(int i=0; i<spoutConfig.getPartitionCount(); ++i) {
      partitions.addPartition(new Partition(spoutConfig, Integer.toString(i)));
    }
    logger.info("created partitions, size=" + spoutConfig.getPartitionCount());
    return partitions;
  }

  @Override
  public boolean isReady(long txid) {
    return true;
  }
}
