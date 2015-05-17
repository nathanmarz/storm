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
package org.apache.storm.eventhubs.spout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.eventhubs.client.Constants;

public class StaticPartitionCoordinator implements IPartitionCoordinator {

  private static final Logger logger = LoggerFactory.getLogger(StaticPartitionCoordinator.class);

  protected final EventHubSpoutConfig config;
  protected final int taskIndex;
  protected final int totalTasks;
  protected final List<IPartitionManager> partitionManagers;
  protected final Map<String, IPartitionManager> partitionManagerMap;
  protected final IStateStore stateStore;

  public StaticPartitionCoordinator(
    EventHubSpoutConfig spoutConfig,
    int taskIndex,
    int totalTasks,
    IStateStore stateStore,
    IPartitionManagerFactory pmFactory,
    IEventHubReceiverFactory recvFactory) {

    this.config = spoutConfig;
    this.taskIndex = taskIndex;
    this.totalTasks = totalTasks;
    this.stateStore = stateStore;
    List<String> partitionIds = calculateParititionIdsToOwn();
    partitionManagerMap = new HashMap<String, IPartitionManager>();
    partitionManagers = new ArrayList<IPartitionManager>();
    
    for (String partitionId : partitionIds) {
      IEventHubReceiver receiver = recvFactory.create(config, partitionId);
      IPartitionManager partitionManager = pmFactory.create(
          config, partitionId, stateStore, receiver);
      partitionManagerMap.put(partitionId, partitionManager);
      partitionManagers.add(partitionManager);
    }
  }

  @Override
  public List<IPartitionManager> getMyPartitionManagers() {
    return partitionManagers;
  }

  @Override
  public IPartitionManager getPartitionManager(String partitionId) {
    return partitionManagerMap.get(partitionId);
  }

  protected List<String> calculateParititionIdsToOwn() {
    List<String> taskPartitions = new ArrayList<String>();
    for (int i = this.taskIndex; i < config.getPartitionCount(); i += this.totalTasks) {
      taskPartitions.add(Integer.toString(i));
      logger.info(String.format("taskIndex %d owns partitionId %d.", this.taskIndex, i));
    }

    return taskPartitions;
  }
}
