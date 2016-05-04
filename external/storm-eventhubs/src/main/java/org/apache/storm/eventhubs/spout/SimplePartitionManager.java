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

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.eventhubs.client.Constants;
import com.microsoft.eventhubs.client.EventHubEnqueueTimeFilter;
import com.microsoft.eventhubs.client.EventHubOffsetFilter;
import com.microsoft.eventhubs.client.IEventHubFilter;

/**
 * A simple partition manager that does not re-send failed messages
 */
public class SimplePartitionManager implements IPartitionManager {
  private static final Logger logger = LoggerFactory.getLogger(SimplePartitionManager.class);
  private static final String statePathPrefix = "/eventhubspout";

  protected final IEventHubReceiver receiver;
  protected String lastOffset = "-1";
  protected String committedOffset = "-1";
  
  protected final EventHubSpoutConfig config;
  private final String partitionId;
  private final IStateStore stateStore;
  private final String statePath;
  
  public SimplePartitionManager(
      EventHubSpoutConfig spoutConfig,
      String partitionId,
      IStateStore stateStore,
      IEventHubReceiver receiver) {
    this.receiver = receiver;
    this.config = spoutConfig;
    this.partitionId = partitionId;
    this.statePath = this.getPartitionStatePath();
    this.stateStore = stateStore;
  }
  
  @Override
  public void open() throws Exception {

    //read from state store, if not found, use startingOffset
    String offset = stateStore.readData(statePath);
    logger.info("read offset from state store: " + offset);
    if(offset == null) {
      offset = Constants.DefaultStartingOffset;
    }

    IEventHubFilter filter;
    if (offset.equals(Constants.DefaultStartingOffset)
        && config.getEnqueueTimeFilter() != 0) {
      filter = new EventHubEnqueueTimeFilter(config.getEnqueueTimeFilter());
    }
    else {
      filter = new EventHubOffsetFilter(offset);
    }

    receiver.open(filter);
  }
  
  @Override
  public void close() {
    this.receiver.close();
    this.checkpoint();
  }
  
  @Override
  public void checkpoint() {
    String completedOffset = getCompletedOffset();
    if(!committedOffset.equals(completedOffset)) {
      logger.info("saving state " + completedOffset);
      stateStore.saveData(statePath, completedOffset);
      committedOffset = completedOffset;
    }
  }
  
  protected String getCompletedOffset() {
    return lastOffset;
  }

  @Override
  public EventData receive() {
    EventData eventData = receiver.receive(5000);
    if (eventData != null) {
      lastOffset = eventData.getMessageId().getOffset();
    }
    return eventData;
  }

  @Override
  public void ack(String offset) {
    //do nothing
  }

  @Override
  public void fail(String offset) {
    logger.warn("fail on " + offset);
    //do nothing
  }
  
  private String getPartitionStatePath() {

    // Partition state path = 
    // "/{prefix}/{topologyName}/{namespace}/{entityPath}/partitions/{partitionId}/state";
    String namespace = config.getNamespace();
    String entityPath = config.getEntityPath();
    String topologyName = config.getTopologyName();

    String partitionStatePath = statePathPrefix + "/" + topologyName + "/" + namespace + "/" + entityPath + "/partitions/" + this.partitionId;

    logger.info("partition state path: " + partitionStatePath);

    return partitionStatePath;
  }
  
  @Override
  public Map getMetricsData() {
    return receiver.getMetricsData();
  }
}
