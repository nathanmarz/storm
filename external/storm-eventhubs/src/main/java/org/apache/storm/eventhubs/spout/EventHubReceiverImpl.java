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

import org.apache.qpid.amqp_1_0.client.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.metric.api.CountMetric;
import org.apache.storm.metric.api.MeanReducer;
import org.apache.storm.metric.api.ReducedMetric;

import com.microsoft.eventhubs.client.Constants;
import com.microsoft.eventhubs.client.EventHubException;
import com.microsoft.eventhubs.client.IEventHubFilter;
import com.microsoft.eventhubs.client.ResilientEventHubReceiver;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.amqp_1_0.type.Section;
import org.apache.qpid.amqp_1_0.type.Symbol;
import org.apache.qpid.amqp_1_0.type.messaging.MessageAnnotations;

public class EventHubReceiverImpl implements IEventHubReceiver {
  private static final Logger logger = LoggerFactory.getLogger(EventHubReceiverImpl.class);
  private static final Symbol OffsetKey = Symbol.valueOf(Constants.OffsetKey);
  private static final Symbol SequenceNumberKey = Symbol.valueOf(Constants.SequenceNumberKey);

  private final String connectionString;
  private final String entityName;
  private final String partitionId;
  private final int defaultCredits;
  private final String consumerGroupName;

  private ResilientEventHubReceiver receiver;
  private ReducedMetric receiveApiLatencyMean;
  private CountMetric receiveApiCallCount;
  private CountMetric receiveMessageCount;

  public EventHubReceiverImpl(EventHubSpoutConfig config, String partitionId) {
    this.connectionString = config.getConnectionString();
    this.entityName = config.getEntityPath();
    this.defaultCredits = config.getReceiverCredits();
    this.partitionId = partitionId;
    this.consumerGroupName = config.getConsumerGroupName();
    receiveApiLatencyMean = new ReducedMetric(new MeanReducer());
    receiveApiCallCount = new CountMetric();
    receiveMessageCount = new CountMetric();
  }

  @Override
  public void open(IEventHubFilter filter) throws EventHubException {
    logger.info("creating eventhub receiver: partitionId=" + partitionId + 
    		", filterString=" + filter.getFilterString());
    long start = System.currentTimeMillis();
    receiver = new ResilientEventHubReceiver(connectionString, entityName,
    		partitionId, consumerGroupName, defaultCredits, filter);
    receiver.initialize();
    
    long end = System.currentTimeMillis();
    logger.info("created eventhub receiver, time taken(ms): " + (end-start));
  }

  @Override
  public void close() {
    if(receiver != null) {
      receiver.close();
      logger.info("closed eventhub receiver: partitionId=" + partitionId );
      receiver = null;
    }
  }
  
  @Override
  public boolean isOpen() {
    return (receiver != null);
  }

  @Override
  public EventData receive(long timeoutInMilliseconds) {
    long start = System.currentTimeMillis();
    Message message = receiver.receive(timeoutInMilliseconds);
    long end = System.currentTimeMillis();
    long millis = (end - start);
    receiveApiLatencyMean.update(millis);
    receiveApiCallCount.incr();
    
    if (message == null) {
      //Temporary workaround for AMQP/EH bug of failing to receive messages
      /*if(timeoutInMilliseconds > 100 && millis < timeoutInMilliseconds/2) {
        throw new RuntimeException(
            "Restart EventHubSpout due to failure of receiving messages in "
            + millis + " millisecond");
      }*/
      return null;
    }

    receiveMessageCount.incr();

    MessageId messageId = createMessageId(message);
    return EventData.create(message, messageId);
  }
  
  private MessageId createMessageId(Message message) {
    String offset = null;
    long sequenceNumber = 0;

    for (Section section : message.getPayload()) {
      if (section instanceof MessageAnnotations) {
        MessageAnnotations annotations = (MessageAnnotations) section;
        HashMap annonationMap = (HashMap) annotations.getValue();

        if (annonationMap.containsKey(OffsetKey)) {
          offset = (String) annonationMap.get(OffsetKey);
        }

        if (annonationMap.containsKey(SequenceNumberKey)) {
          sequenceNumber = (Long) annonationMap.get(SequenceNumberKey);
        }
      }
    }

    return MessageId.create(partitionId, offset, sequenceNumber);
  }

  @Override
  public Map getMetricsData() {
    Map ret = new HashMap();
    ret.put(partitionId + "/receiveApiLatencyMean", receiveApiLatencyMean.getValueAndReset());
    ret.put(partitionId + "/receiveApiCallCount", receiveApiCallCount.getValueAndReset());
    ret.put(partitionId + "/receiveMessageCount", receiveMessageCount.getValueAndReset());
    return ret;
  }
}
