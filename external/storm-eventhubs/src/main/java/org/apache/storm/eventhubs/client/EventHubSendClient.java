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

import org.apache.storm.eventhubs.spout.EventHubSpoutConfig;

public class EventHubSendClient {
  
  public static void main(String[] args) throws Exception {
    
    if (args == null || args.length < 7) {
      throw new IllegalArgumentException(
        "arguments are missing. [username] [password] [namespace] [entityPath] [partitionId] [messageSize] [messageCount] are required.");
    }
    
    String username = args[0];
    String password = args[1];
    String namespace = args[2];
    String entityPath = args[3];
    String partitionId = args[4];
    int messageSize = Integer.parseInt(args[5]);
    int messageCount = Integer.parseInt(args[6]);
    assert(messageSize > 0);
    assert(messageCount > 0);
    
    if (partitionId.equals("-1")) {
      // -1 means we want to send data to partitions in round-robin fashion.
      partitionId = null;
    }
    
    try {
      String connectionString = EventHubSpoutConfig.buildConnectionString(username, password, namespace);
      EventHubClient client = EventHubClient.create(connectionString, entityPath);
      EventHubSender sender = client.createPartitionSender(partitionId);
      
      StringBuilder sb = new StringBuilder(messageSize);
      for(int i=1; i<messageCount+1; ++i) {
        while(sb.length() < messageSize) {
          sb.append(" current message: " + i);
        }
        sb.setLength(messageSize);
        sender.send(sb.toString());
        sb.setLength(0);
        if(i % 1000 == 0) {
          System.out.println("Number of messages sent: " + i);
        }
      }
      System.out.println("Total Number of messages sent: " + messageCount);
    } catch (Exception e) {
      System.out.println("Exception: " + e.getMessage());
    }
    
    System.out.println("done");
  }
}
