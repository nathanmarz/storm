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


public class EventHubReceiverFilter implements IEventHubReceiverFilter {
  String offset = null;
  long enqueueTime = 0;
  public EventHubReceiverFilter() {
    
  }
  
  public EventHubReceiverFilter(String offset) {
    //Creates offset only filter
    this.offset = offset;
  }
  
  public EventHubReceiverFilter(long enqueueTime) {
    //Creates enqueue time only filter
    this.enqueueTime = enqueueTime;
  }
  
  public void setOffset(String offset) {
    this.offset = offset;
  }
  
  public void setEnqueueTime(long enqueueTime) {
    this.enqueueTime = enqueueTime;
  }
  
  @Override
  public String getOffset() {
    return offset;
  }

  @Override
  public long getEnqueueTime() {
    return enqueueTime;
  }

}
