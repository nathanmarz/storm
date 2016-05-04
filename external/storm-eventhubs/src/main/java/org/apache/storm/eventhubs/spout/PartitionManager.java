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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeSet;

public class PartitionManager extends SimplePartitionManager {
  private static final Logger logger = LoggerFactory.getLogger(PartitionManager.class);
  private final int ehReceiveTimeoutMs = 5000;

  //all sent events are stored in pending
  private final Map<String, EventData> pending;
  //all failed events are put in toResend, which is sorted by event's offset
  private final TreeSet<EventData> toResend;

  public PartitionManager(
    EventHubSpoutConfig spoutConfig,
    String partitionId,
    IStateStore stateStore,
    IEventHubReceiver receiver) {

    super(spoutConfig, partitionId, stateStore, receiver);
    
    this.pending = new LinkedHashMap<String, EventData>();
    this.toResend = new TreeSet<EventData>();
  }

  @Override
  public EventData receive() {
    if(pending.size() >= config.getMaxPendingMsgsPerPartition()) {
      return null;
    }

    EventData eventData;
    if (toResend.isEmpty()) {
      eventData = receiver.receive(ehReceiveTimeoutMs);
    } else {
      eventData = toResend.pollFirst();
    }

    if (eventData != null) {
      lastOffset = eventData.getMessageId().getOffset();
      pending.put(lastOffset, eventData);
    }

    return eventData;
  }

  @Override
  public void ack(String offset) {
    pending.remove(offset);
  }

  @Override
  public void fail(String offset) {
    logger.warn("fail on " + offset);
    EventData eventData = pending.remove(offset);
    toResend.add(eventData);
  }
  
  @Override
  protected String getCompletedOffset() {
    String offset = null;
    
    if(pending.size() > 0) {
      //find the smallest offset in pending list
      offset = pending.keySet().iterator().next();
    }
    if(toResend.size() > 0) {
      //find the smallest offset in toResend list
      String offset2 = toResend.first().getMessageId().getOffset();
      if(offset == null || offset2.compareTo(offset) < 0) {
        offset = offset2;
      }
    }
    if(offset == null) {
      offset = lastOffset;
    }
    return offset;
  }
}
