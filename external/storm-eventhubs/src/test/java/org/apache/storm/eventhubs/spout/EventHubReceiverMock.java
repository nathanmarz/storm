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
import java.util.List;
import java.util.Map;

import org.apache.storm.eventhubs.spout.MessageId;
import org.apache.storm.eventhubs.spout.EventData;
import org.apache.storm.eventhubs.spout.IEventHubReceiver;
import org.apache.qpid.amqp_1_0.client.Message;
import org.apache.qpid.amqp_1_0.jms.impl.TextMessageImpl;
import org.apache.qpid.amqp_1_0.type.Binary;
import org.apache.qpid.amqp_1_0.type.Section;
import org.apache.qpid.amqp_1_0.type.messaging.Data;

import com.microsoft.eventhubs.client.EventHubException;
import com.microsoft.eventhubs.client.EventHubOffsetFilter;
import com.microsoft.eventhubs.client.IEventHubFilter;

/**
 * A mock receiver that emits fake data with offset starting from given offset
 * and increase by 1 each time.
 */
public class EventHubReceiverMock implements IEventHubReceiver {
  private static boolean isPaused = false;
  private final String partitionId;
  private long currentOffset;
  private boolean isOpen;

  public EventHubReceiverMock(String pid) {
    partitionId = pid;
    isPaused = false;
  }
  
  /**
   * Use this method to pause/resume all the receivers.
   * If paused all receiver will return null.
   * @param val
   */
  public static void setPause(boolean val) {
    isPaused = val;
  }

  @Override
  public void open(IEventHubFilter filter) throws EventHubException {
    currentOffset = Long.parseLong(filter.getFilterValue());
    isOpen = true;
  }

  @Override
  public void close() {
    isOpen = false;
  }
  
  @Override
  public boolean isOpen() {
    return isOpen;
  }

  @Override
  public EventData receive(long timeoutInMilliseconds) {
    if(isPaused) {
      return null;
    }

    currentOffset++;
    List<Section> body = new ArrayList<Section>();
    //the body of the message is "message" + currentOffset, e.g. "message123"
    body.add(new Data(new Binary(("message" + currentOffset).getBytes())));
    Message m = new Message(body);
    MessageId mid = new MessageId(partitionId, "" + currentOffset, currentOffset);
    EventData ed = new EventData(m, mid);
    return ed;
  }
  
  @Override
  public Map getMetricsData() {
    return null;
  }
}
