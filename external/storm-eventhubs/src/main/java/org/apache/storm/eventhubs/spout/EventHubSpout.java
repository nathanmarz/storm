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

import backtype.storm.Config;
import backtype.storm.metric.api.IMetric;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.qpid.amqp_1_0.client.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventHubSpout extends BaseRichSpout {

  private static final Logger logger = LoggerFactory.getLogger(EventHubSpout.class);

  private final UUID instanceId;
  private final EventHubSpoutConfig eventHubConfig;
  private final IEventDataScheme scheme;
  private final int checkpointIntervalInSeconds;

  private IStateStore stateStore;
  private IPartitionCoordinator partitionCoordinator;
  private IPartitionManagerFactory pmFactory;
  private IEventHubReceiverFactory recvFactory;
  private SpoutOutputCollector collector;
  private long lastCheckpointTime;
  private int currentPartitionIndex = -1;

  public EventHubSpout(String username, String password, String namespace,
      String entityPath, int partitionCount) {
    this(new EventHubSpoutConfig(username, password, namespace, entityPath, partitionCount));
  }

  public EventHubSpout(EventHubSpoutConfig spoutConfig) {
    this(spoutConfig, null, null, null);
  }
  
  public EventHubSpout(EventHubSpoutConfig spoutConfig,
      IStateStore store,
      IPartitionManagerFactory pmFactory,
      IEventHubReceiverFactory recvFactory) {
    this.eventHubConfig = spoutConfig;
    this.scheme = spoutConfig.getEventDataScheme();
    this.instanceId = UUID.randomUUID();
    this.checkpointIntervalInSeconds = spoutConfig.getCheckpointIntervalInSeconds();
    this.lastCheckpointTime = System.currentTimeMillis();
    stateStore = store;
    this.pmFactory = pmFactory;
    if(this.pmFactory == null) {
      this.pmFactory = new IPartitionManagerFactory() {
        @Override
        public IPartitionManager create(EventHubSpoutConfig spoutConfig,
            String partitionId, IStateStore stateStore,
            IEventHubReceiver receiver) {
          return new PartitionManager(spoutConfig, partitionId,
              stateStore, receiver);
        }
      };
    }
    this.recvFactory = recvFactory;
    if(this.recvFactory == null) {
      this.recvFactory = new IEventHubReceiverFactory() {
        @Override
        public IEventHubReceiver create(EventHubSpoutConfig spoutConfig,
            String partitionId) {
          return new EventHubReceiverImpl(spoutConfig, partitionId);
        }
      };
    }
    
  }
  
  /**
   * This is a extracted method that is easy to test
   * @param config
   * @param totalTasks
   * @param taskIndex
   * @param collector
   * @throws Exception
   */
  public void preparePartitions(Map config, int totalTasks, int taskIndex, SpoutOutputCollector collector) throws Exception {
    this.collector = collector;
    if(stateStore == null) {
      String zkEndpointAddress = eventHubConfig.getZkConnectionString();
      if (zkEndpointAddress == null || zkEndpointAddress.length() == 0) {
        //use storm's zookeeper servers if not specified.
        @SuppressWarnings("unchecked")
        List<String> zkServers = (List<String>) config.get(Config.STORM_ZOOKEEPER_SERVERS);
        Integer zkPort = ((Number) config.get(Config.STORM_ZOOKEEPER_PORT)).intValue();
        StringBuilder sb = new StringBuilder();
        for (String zk : zkServers) {
          if (sb.length() > 0) {
            sb.append(',');
          }
          sb.append(zk+":"+zkPort);
        }
        zkEndpointAddress = sb.toString();
      }
      stateStore = new ZookeeperStateStore(zkEndpointAddress,
          (Integer)config.get(Config.STORM_ZOOKEEPER_RETRY_TIMES),
          (Integer)config.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL));
    }
    stateStore.open();

    partitionCoordinator = new StaticPartitionCoordinator(
        eventHubConfig, taskIndex, totalTasks, stateStore, pmFactory, recvFactory);

    for (IPartitionManager partitionManager : 
      partitionCoordinator.getMyPartitionManagers()) {
      partitionManager.open();
    }
  }

  @Override
  public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
    logger.info("begin: open()");
    String topologyName = (String) config.get(Config.TOPOLOGY_NAME);
    eventHubConfig.setTopologyName(topologyName);

    int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
    int taskIndex = context.getThisTaskIndex();
    if (totalTasks > eventHubConfig.getPartitionCount()) {
      throw new RuntimeException("total tasks of EventHubSpout is greater than partition count.");
    }

    logger.info(String.format("topologyName: %s, totalTasks: %d, taskIndex: %d", topologyName, totalTasks, taskIndex));

    try {
      preparePartitions(config, totalTasks, taskIndex, collector);
    } catch (Exception e) {
      logger.error(e.getMessage());
      throw new RuntimeException(e);
    }
    
    //register metrics
    context.registerMetric("EventHubReceiver", new IMetric() {
      @Override
      public Object getValueAndReset() {
          Map concatMetricsDataMaps = new HashMap();
          for (IPartitionManager partitionManager : 
            partitionCoordinator.getMyPartitionManagers()) {
            concatMetricsDataMaps.putAll(partitionManager.getMetricsData());
          }
          return concatMetricsDataMaps;
      }
    }, (Integer)config.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS));
    logger.info("end open()");
  }

  @Override
  public void nextTuple() {
    EventData eventData = null;

    List<IPartitionManager> partitionManagers = partitionCoordinator.getMyPartitionManagers();
    for (int i = 0; i < partitionManagers.size(); i++) {
      currentPartitionIndex = (currentPartitionIndex + 1) % partitionManagers.size();
      IPartitionManager partitionManager = partitionManagers.get(currentPartitionIndex);

      if (partitionManager == null) {
        throw new RuntimeException("partitionManager doesn't exist.");
      }

      eventData = partitionManager.receive();

      if (eventData != null) {
        break;
      }
    }


    if (eventData != null) {
      MessageId messageId = eventData.getMessageId();
      Message message = eventData.getMessage();

      List<Object> tuples = scheme.deserialize(message);

      if (tuples != null) {
        collector.emit(tuples, messageId);
      }
    }
    
    checkpointIfNeeded();

    // We don't need to sleep here because the IPartitionManager.receive() is
    // a blocked call so it's fine to call this function in a tight loop.
  }

  @Override
  public void ack(Object msgId) {
    MessageId messageId = (MessageId) msgId;
    IPartitionManager partitionManager = partitionCoordinator.getPartitionManager(messageId.getPartitionId());
    String offset = messageId.getOffset();
    partitionManager.ack(offset);
  }

  @Override
  public void fail(Object msgId) {
    MessageId messageId = (MessageId) msgId;
    IPartitionManager partitionManager = partitionCoordinator.getPartitionManager(messageId.getPartitionId());
    String offset = messageId.getOffset();
    partitionManager.fail(offset);
  }

  @Override
  public void deactivate() {
    // let's checkpoint so that we can get the last checkpoint when restarting.
    checkpoint();
  }

  @Override
  public void close() {
    for (IPartitionManager partitionManager : 
      partitionCoordinator.getMyPartitionManagers()) {
      partitionManager.close();
    }
    stateStore.close();
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(scheme.getOutputFields());
  }

  private void checkpointIfNeeded() {
    long nextCheckpointTime = lastCheckpointTime + checkpointIntervalInSeconds * 1000;
    if (nextCheckpointTime < System.currentTimeMillis()) {

      checkpoint();
      lastCheckpointTime = System.currentTimeMillis();
    }
  }
  
  private void checkpoint() {
    for (IPartitionManager partitionManager : 
      partitionCoordinator.getMyPartitionManagers()) {
      partitionManager.checkpoint();
    }
  }
}
