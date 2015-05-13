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

import backtype.storm.metric.api.CountMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.ReducedMetric;

import org.apache.storm.eventhubs.client.Constants;
import org.apache.storm.eventhubs.client.EventHubClient;
import org.apache.storm.eventhubs.client.EventHubException;
import org.apache.storm.eventhubs.client.EventHubReceiver;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.amqp_1_0.type.Section;
import org.apache.qpid.amqp_1_0.type.Symbol;
import org.apache.qpid.amqp_1_0.type.messaging.MessageAnnotations;

public class EventHubReceiverImpl implements IEventHubReceiver {
  private static final Logger logger = LoggerFactory.getLogger(EventHubReceiverImpl.class);
  private static final Symbol OffsetKey = Symbol.valueOf("x-opt-offset");
  private static final Symbol SequenceNumberKey = Symbol.valueOf("x-opt-sequence-number");

  private final String connectionString;
  private final String entityName;
  private final String partitionId;
  private final int defaultCredits;

  private EventHubReceiver receiver;
  private String lastOffset = null;
  private ReducedMetric receiveApiLatencyMean;
  private CountMetric receiveApiCallCount;
  private CountMetric receiveMessageCount;

  public EventHubReceiverImpl(EventHubSpoutConfig config, String partitionId) {
    this.connectionString = config.getConnectionString();
    this.entityName = config.getEntityPath();
    this.defaultCredits = config.getReceiverCredits();
    this.partitionId = partitionId;
    receiveApiLatencyMean = new ReducedMetric(new MeanReducer());
    receiveApiCallCount = new CountMetric();
    receiveMessageCount = new CountMetric();
  }

  @Override
  public void open(IEventHubReceiverFilter filter) throws EventHubException {
    logger.info("creating eventhub receiver: partitionId=" + partitionId + ", offset=" + filter.getOffset()
        + ", enqueueTime=" + filter.getEnqueueTime());
    long start = System.currentTimeMillis();
    EventHubClient eventHubClient = EventHubClient.create(connectionString, entityName);
    if(filter.getOffset() != null) {
      receiver = eventHubClient.getDefaultConsumerGroup().createReceiver(partitionId, filter.getOffset(), defaultCredits);
    }
    else if(filter.getEnqueueTime() != 0) {
      receiver = eventHubClient.getDefaultConsumerGroup().createReceiver(partitionId, filter.getEnqueueTime(), defaultCredits);
    }
    else {
      logger.error("Invalid IEventHubReceiverFilter, use default offset as filter");
      receiver = eventHubClient.getDefaultConsumerGroup().createReceiver(partitionId, Constants.DefaultStartingOffset, defaultCredits);
    }
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
      return null;
    }
    receiveMessageCount.incr();

    //logger.info(String.format("received a message. PartitionId: %s, Offset: %s", partitionId, this.lastOffset));
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
