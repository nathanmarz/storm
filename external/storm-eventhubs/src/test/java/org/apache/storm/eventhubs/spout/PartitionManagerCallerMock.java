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

import org.apache.storm.eventhubs.spout.PartitionManager;
import org.apache.storm.eventhubs.spout.EventData;
import org.apache.storm.eventhubs.spout.EventHubSpoutConfig;

/**
 * This mock exercises PartitionManager
 */
public class PartitionManagerCallerMock {
  public static final String statePath = "/eventhubspout/TestTopo/namespace/entityname/partitions/1";
  private IPartitionManager pm;
  private IStateStore stateStore;

  public PartitionManagerCallerMock(String partitionId) {
    this(partitionId, 0);
  }
  
  public PartitionManagerCallerMock(String partitionId, long enqueueTimeFilter) {
    EventHubReceiverMock receiver = new EventHubReceiverMock(partitionId);
    EventHubSpoutConfig conf = new EventHubSpoutConfig("username", "password",
      "namespace", "entityname", 16, "zookeeper", 10, 1024, 1024, enqueueTimeFilter);
    conf.setTopologyName("TestTopo");
    stateStore = new StateStoreMock();
    this.pm = new PartitionManager(conf, partitionId, stateStore, receiver);
    
    stateStore.open();
    try {
      pm.open();
    }
    catch (Exception ex) {
    }
  }
  
  /**
   * Execute a sequence of calls to Partition Manager.
   * 
   * @param callSequence: is represented as a string of commands, 
   * e.g. "r,r,r,r,a1,f2,...". The commands are:
   * r[N]: receive() called N times
   * aX: ack(X)
   * fY: fail(Y)
   * 
   * @return the sequence of messages the receive call returns
   */
  public String execute(String callSequence) {
    
    String[] cmds = callSequence.split(",");
    StringBuilder ret = new StringBuilder();
    for(String cmd : cmds) {
      if(cmd.startsWith("r")) {
        int count = 1;
        if(cmd.length() > 1) {
          count = Integer.parseInt(cmd.substring(1));
        }
        for(int i=0; i<count; ++i) {
          EventData ed = pm.receive();
          if(ed == null) {
            ret.append("null,");
          }
          else {
            ret.append(ed.getMessageId().getOffset());
            ret.append(",");
          }
        }
      }
      else if(cmd.startsWith("a")) {
        pm.ack(cmd.substring(1));
      }
      else if(cmd.startsWith("f")) {
        pm.fail(cmd.substring(1));
      }
    }
    if(ret.length() > 0) {
      ret.setLength(ret.length()-1);
    }
    return ret.toString();
  }
  
  /**
   * Exercise the IPartitionManager.checkpoint() method
   * @return the offset that we write to state store
   */
  public String checkpoint() {
    pm.checkpoint();
    return stateStore.readData(statePath);
  }
}
