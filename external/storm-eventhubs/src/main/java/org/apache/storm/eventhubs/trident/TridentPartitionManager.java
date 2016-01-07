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

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.eventhubs.client.Constants;
import com.microsoft.eventhubs.client.EventHubEnqueueTimeFilter;
import com.microsoft.eventhubs.client.EventHubException;
import com.microsoft.eventhubs.client.EventHubOffsetFilter;

import org.apache.storm.eventhubs.spout.EventData;
import org.apache.storm.eventhubs.spout.EventHubSpoutConfig;
import org.apache.storm.eventhubs.spout.IEventHubReceiver;

public class TridentPartitionManager implements ITridentPartitionManager {
  private static final Logger logger = LoggerFactory.getLogger(TridentPartitionManager.class);
  private final int receiveTimeoutMs = 5000;
  private final IEventHubReceiver receiver;
  private final EventHubSpoutConfig spoutConfig;
  private String lastOffset = Constants.DefaultStartingOffset;
  
  public TridentPartitionManager(EventHubSpoutConfig spoutConfig, IEventHubReceiver receiver) {
    this.receiver = receiver;
    this.spoutConfig = spoutConfig;
  }
  
  @Override
  public boolean open(String offset) {
    try {
      if((offset == null || offset.equals(Constants.DefaultStartingOffset)) 
        && spoutConfig.getEnqueueTimeFilter() != 0) {
          receiver.open(new EventHubEnqueueTimeFilter(spoutConfig.getEnqueueTimeFilter()));
      }
      else {
        receiver.open(new EventHubOffsetFilter(offset));
      }
      lastOffset = offset;
      return true;
    }
    catch(EventHubException ex) {
      logger.error("failed to open eventhub receiver: " + ex.getMessage());
      return false;
    }
  }
  
  @Override
  public void close() {
    receiver.close();
  }
  
  @Override
  public List<EventData> receiveBatch(String offset, int count) {
    List<EventData> batch = new ArrayList<EventData>(count);
    if(!offset.equals(lastOffset) || !receiver.isOpen()) {
      //re-establish connection to eventhub servers using the right offset
      //TBD: might be optimized with cache.
      close();
      if(!open(offset)) {
        return batch;
      }
    }
    
    for(int i=0; i<count; ++i) {
      EventData ed = receiver.receive(receiveTimeoutMs);
      if(ed == null) {
        break;
      }
      batch.add(ed);
      lastOffset = ed.getMessageId().getOffset();
    }
    return batch;
  }
}
