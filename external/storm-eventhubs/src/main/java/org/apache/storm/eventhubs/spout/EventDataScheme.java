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

import org.apache.storm.tuple.Fields;
import java.util.ArrayList;
import java.util.List;
import org.apache.qpid.amqp_1_0.client.Message;
import org.apache.qpid.amqp_1_0.type.Section;
import org.apache.qpid.amqp_1_0.type.messaging.AmqpValue;
import org.apache.qpid.amqp_1_0.type.messaging.Data;

public class EventDataScheme implements IEventDataScheme {

  private static final long serialVersionUID = 1L;

  @Override
  public List<Object> deserialize(Message message) {
    List<Object> fieldContents = new ArrayList<Object>();

    for (Section section : message.getPayload()) {
      if (section instanceof Data) {
        Data data = (Data) section;
        fieldContents.add(new String(data.getValue().getArray()));
        return fieldContents;
      } else if (section instanceof AmqpValue) {
        AmqpValue amqpValue = (AmqpValue) section;
        fieldContents.add(amqpValue.getValue().toString());
        return fieldContents;
      }
    }

    return null;
  }

  @Override
  public Fields getOutputFields() {
    return new Fields(FieldConstants.Message);
  }
}
