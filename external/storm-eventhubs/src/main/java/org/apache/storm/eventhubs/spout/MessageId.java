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

public class MessageId {

  private final String partitionId;
  private final String offset;
  private final long sequenceNumber;

  public MessageId(
    String partitionId,
    String offset,
    long sequenceNumber) {
    this.partitionId = partitionId;
    this.offset = offset;
    this.sequenceNumber = sequenceNumber;
  }

  public static MessageId create(String partitionId, String offset, long sequenceNumber) {
    return new MessageId(partitionId, offset, sequenceNumber);
  }

  public String getPartitionId() {
    return this.partitionId;
  }

  public String getOffset() {
    return this.offset;
  }

  public Long getSequenceNumber() {
    return this.sequenceNumber;
  }
  
  @Override
  public String toString() {
    return String.format("PartitionId: %s, Offset: %s, SequenceNumber: %s",
      this.partitionId, this.offset, this.sequenceNumber);
  }
}
