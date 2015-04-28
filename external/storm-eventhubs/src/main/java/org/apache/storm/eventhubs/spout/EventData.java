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

public class EventData implements Comparable<EventData> {
  private final Message message;
  private final MessageId messageId;

  public EventData(Message message, MessageId messageId) {
    this.message = message;
    this.messageId = messageId;
  }

  public static EventData create(Message message, MessageId messageId) {
    return new EventData(message, messageId);
  }

  public Message getMessage() {
    return this.message;
  }

  public MessageId getMessageId() {
    return this.messageId;
  }

  @Override
  public int compareTo(EventData ed) {
    return messageId.getSequenceNumber().
        compareTo(ed.getMessageId().getSequenceNumber());
  }
}
