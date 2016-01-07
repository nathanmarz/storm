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
package org.apache.storm.eventhubs.samples;

import java.io.Serializable;

import org.apache.storm.eventhubs.spout.EventHubSpout;
import org.apache.storm.eventhubs.spout.EventHubSpoutConfig;
import org.apache.storm.eventhubs.spout.IEventHubReceiver;
import org.apache.storm.eventhubs.spout.IPartitionManager;
import org.apache.storm.eventhubs.spout.IPartitionManagerFactory;
import org.apache.storm.eventhubs.spout.IStateStore;
import org.apache.storm.eventhubs.spout.SimplePartitionManager;

public class AtMostOnceEventCount extends EventCount implements Serializable {
  @Override
  protected EventHubSpout createEventHubSpout() {
    IPartitionManagerFactory pmFactory = new IPartitionManagerFactory() {
      private static final long serialVersionUID = 1L;

      @Override
      public IPartitionManager create(EventHubSpoutConfig spoutConfig,
          String partitionId, IStateStore stateStore,
          IEventHubReceiver receiver) {
        return new SimplePartitionManager(spoutConfig, partitionId,
            stateStore, receiver);
      }
    };
    EventHubSpout eventHubSpout = new EventHubSpout(
        spoutConfig, null, pmFactory, null);
    return eventHubSpout;
  }
  
  public static void main(String[] args) throws Exception {
    AtMostOnceEventCount scenario = new AtMostOnceEventCount();

    scenario.runScenario(args);
  }
}
