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
package org.apache.storm.eventhubs.bolt;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.eventhubs.client.EventHubClient;
import com.microsoft.eventhubs.client.EventHubException;
import com.microsoft.eventhubs.client.EventHubSender;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * A bolt that writes event message to EventHub.
 */
public class EventHubBolt extends BaseRichBolt {
  private static final long serialVersionUID = 1L;
  private static final Logger logger = LoggerFactory
      .getLogger(EventHubBolt.class);
  
  protected OutputCollector collector;
  protected EventHubSender sender;
  protected EventHubBoltConfig boltConfig;
  
  
  public EventHubBolt(String connectionString, String entityPath) {
    boltConfig = new EventHubBoltConfig(connectionString, entityPath);
  }

  public EventHubBolt(String userName, String password, String namespace,
      String entityPath, boolean partitionMode) {
    boltConfig = new EventHubBoltConfig(userName, password, namespace,
        entityPath, partitionMode);
  }
  
  public EventHubBolt(EventHubBoltConfig config) {
    boltConfig = config;
  }

  @Override
  public void prepare(Map config, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
    String myPartitionId = null;
    if(boltConfig.getPartitionMode()) {
      //We can use the task index (starting from 0) as the partition ID
      myPartitionId = "" + context.getThisTaskIndex();
    }
    logger.info("creating sender: " + boltConfig.getConnectionString()
        + ", " + boltConfig.getEntityPath() + ", " + myPartitionId);
    try {
      EventHubClient eventHubClient = EventHubClient.create(
          boltConfig.getConnectionString(), boltConfig.getEntityPath());
      sender = eventHubClient.createPartitionSender(myPartitionId);
    }
    catch(Exception ex) {
      logger.error(ex.getMessage());
      throw new RuntimeException(ex);
    }

  }

  @Override
  public void execute(Tuple tuple) {
    try {
      sender.send(boltConfig.getEventDataFormat().serialize(tuple));
      collector.ack(tuple);
    }
    catch(EventHubException ex) {
      logger.error(ex.getMessage());
      collector.fail(tuple);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    
  }

}
