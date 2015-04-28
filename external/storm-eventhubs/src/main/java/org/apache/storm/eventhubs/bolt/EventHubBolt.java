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

import org.apache.storm.eventhubs.client.EventHubClient;
import org.apache.storm.eventhubs.client.EventHubException;
import org.apache.storm.eventhubs.client.EventHubSender;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * A bolt that writes message to EventHub.
 * We assume the incoming tuple has only one field which is a string.
 */
public class EventHubBolt extends BaseBasicBolt {
  private static final long serialVersionUID = 1L;
  private static final Logger logger = LoggerFactory
      .getLogger(EventHubBolt.class);
  
  private EventHubSender sender;
  private String connectionString;
  private String entityPath;
  
  public EventHubBolt(String connectionString, String entityPath) {
    this.connectionString = connectionString;
    this.entityPath = entityPath;
  }
  
  @Override
  public void prepare(Map config, TopologyContext context) {
    try {
      EventHubClient eventHubClient = EventHubClient.create(connectionString, entityPath);
      sender = eventHubClient.createPartitionSender(null);
    }
    catch(Exception ex) {
      logger.error(ex.getMessage());
      throw new RuntimeException(ex);
    }

  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    try {
      sender.send((String)tuple.getValue(0));
    }
    catch(EventHubException ex) {
      logger.error(ex.getMessage());
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    
  }

}
