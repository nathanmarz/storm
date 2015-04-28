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
package org.apache.storm.eventhubs.client;

import java.util.Collections;
import java.util.Map;
import org.apache.qpid.amqp_1_0.client.AcknowledgeMode;
import org.apache.qpid.amqp_1_0.client.ConnectionErrorException;
import org.apache.qpid.amqp_1_0.client.Message;
import org.apache.qpid.amqp_1_0.client.Receiver;
import org.apache.qpid.amqp_1_0.client.Session;
import org.apache.qpid.amqp_1_0.type.Symbol;
import org.apache.qpid.amqp_1_0.type.UnsignedInteger;
import org.apache.qpid.amqp_1_0.type.messaging.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class EventHubReceiver {

  private static final Logger logger = LoggerFactory
      .getLogger(EventHubReceiver.class);
  private static final String linkName = "eventhubs-receiver-link";

  private final Session session;
  private final String entityPath;
  private final String consumerGroupName;
  private final String partitionId;
  private final String consumerAddress;
  private final Map<Symbol, Filter> filters;
  private final int defaultCredits;

  private Receiver receiver;
  private boolean isClosed;

  public EventHubReceiver(Session session, String entityPath,
      String consumerGroupName, String partitionId, String filterStr, int defaultCredits)
      throws EventHubException {

    this.session = session;
    this.entityPath = entityPath;
    this.consumerGroupName = consumerGroupName;
    this.partitionId = partitionId;
    this.consumerAddress = this.getConsumerAddress();
    this.filters = Collections.singletonMap(
        Symbol.valueOf(Constants.SelectorFilterName),
        (Filter) new SelectorFilter(filterStr));
    logger.info("receiver filter string: " + filterStr);
    this.defaultCredits = defaultCredits;

    this.ensureReceiverCreated();
  }

  // receive without timeout means wait until a message is delivered.
  public Message receive() {
    return this.receive(-1L);
  }

  public Message receive(long waitTimeInMilliseconds) {

    this.checkIfClosed();

    Message message = this.receiver.receive(waitTimeInMilliseconds);

    if (message != null) {
      // Let's acknowledge a message although EH service doesn't need it
      // to avoid AMQP flow issue.
      receiver.acknowledge(message);

      return message;
    } else {
      this.checkError();
    }

    return null;
  }

  public void close() {
    if (!isClosed) {
      receiver.close();
      isClosed = true;
    }
  }

  private String getConsumerAddress() {
    return String.format(Constants.ConsumerAddressFormatString,
        entityPath, consumerGroupName, partitionId);
  }

  private void ensureReceiverCreated() throws EventHubException {
    try {
      logger.info("defaultCredits: " + defaultCredits);
      receiver = session.createReceiver(consumerAddress,
          AcknowledgeMode.ALO, linkName, false, filters, null);
      receiver.setCredit(UnsignedInteger.valueOf(defaultCredits), true);
    } catch (ConnectionErrorException e) {
      // caller (EventHubSpout) will log the error
      throw new EventHubException(e);
    }
  }

  private void checkError() {
    org.apache.qpid.amqp_1_0.type.transport.Error error = this.receiver.getError();
    if (error != null) {
      String errorMessage = error.toString();
      logger.error(errorMessage);
      this.close();

      throw new RuntimeException(errorMessage);
    } else {
      // adding a sleep here to avoid any potential tight-loop issue.
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        logger.error(e.toString());
      }
    }
  }
  
  private void checkIfClosed() {
    if (this.isClosed) {
      throw new RuntimeException("receiver was closed.");
    }
  }
}
