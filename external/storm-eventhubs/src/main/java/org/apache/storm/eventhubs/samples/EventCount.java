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

import org.apache.storm.StormSubmitter;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

import org.apache.storm.eventhubs.samples.bolt.GlobalCountBolt;
import org.apache.storm.eventhubs.samples.bolt.PartialCountBolt;
import org.apache.storm.eventhubs.spout.EventHubSpout;
import org.apache.storm.eventhubs.spout.EventHubSpoutConfig;

import java.io.FileReader;
import java.util.Properties;

/**
 * The basic scenario topology that uses EventHubSpout with PartialCountBolt
 * and GlobalCountBolt.
 * To submit this topology:
 * storm jar {jarfile} {classname} {topologyname} {spoutconffile}
 */
public class EventCount {
  protected EventHubSpoutConfig spoutConfig;
  protected int numWorkers;
  
  public EventCount() {
  }
  
  protected void readEHConfig(String[] args) throws Exception {
	  Properties properties = new Properties();
    if(args.length > 1) {
      properties.load(new FileReader(args[1]));
    }
    else {
      properties.load(EventCount.class.getClassLoader().getResourceAsStream(
          "Config.properties"));
    }

    String username = properties.getProperty("eventhubspout.username");
    String password = properties.getProperty("eventhubspout.password");
    String namespaceName = properties.getProperty("eventhubspout.namespace");
    String entityPath = properties.getProperty("eventhubspout.entitypath");
    String targetFqnAddress = properties.getProperty("eventhubspout.targetfqnaddress");
    String zkEndpointAddress = properties.getProperty("zookeeper.connectionstring");
    int partitionCount = Integer.parseInt(properties.getProperty("eventhubspout.partitions.count"));
    int checkpointIntervalInSeconds = Integer.parseInt(properties.getProperty("eventhubspout.checkpoint.interval"));
    int receiverCredits = Integer.parseInt(properties.getProperty("eventhub.receiver.credits"));
    String maxPendingMsgsPerPartitionStr = properties.getProperty("eventhubspout.max.pending.messages.per.partition");
    if(maxPendingMsgsPerPartitionStr == null) {
      maxPendingMsgsPerPartitionStr = "1024";
    }
    int maxPendingMsgsPerPartition = Integer.parseInt(maxPendingMsgsPerPartitionStr);
    String enqueueTimeDiffStr = properties.getProperty("eventhub.receiver.filter.timediff");
    if(enqueueTimeDiffStr == null) {
      enqueueTimeDiffStr = "0";
    }
    int enqueueTimeDiff = Integer.parseInt(enqueueTimeDiffStr);
    long enqueueTimeFilter = 0;
    if(enqueueTimeDiff != 0) {
      enqueueTimeFilter = System.currentTimeMillis() - enqueueTimeDiff*1000;
    }
    String consumerGroupName = properties.getProperty("eventhubspout.consumer.group.name");
    
    System.out.println("Eventhub spout config: ");
    System.out.println("  partition count: " + partitionCount);
    System.out.println("  checkpoint interval: " + checkpointIntervalInSeconds);
    System.out.println("  receiver credits: " + receiverCredits);
    spoutConfig = new EventHubSpoutConfig(username, password,
      namespaceName, entityPath, partitionCount, zkEndpointAddress,
      checkpointIntervalInSeconds, receiverCredits, maxPendingMsgsPerPartition,
      enqueueTimeFilter);

    if(targetFqnAddress != null)
    {
      spoutConfig.setTargetAddress(targetFqnAddress);      
    }
    spoutConfig.setConsumerGroupName(consumerGroupName);

    //set the number of workers to be the same as partition number.
    //the idea is to have a spout and a partial count bolt co-exist in one
    //worker to avoid shuffling messages across workers in storm cluster.
    numWorkers = spoutConfig.getPartitionCount();
    
    if(args.length > 0) {
      //set topology name so that sample Trident topology can use it as stream name.
      spoutConfig.setTopologyName(args[0]);
    }
	}
  
  protected EventHubSpout createEventHubSpout() {
    EventHubSpout eventHubSpout = new EventHubSpout(spoutConfig);
    return eventHubSpout;
  }
	
  protected StormTopology buildTopology(EventHubSpout eventHubSpout) {
    TopologyBuilder topologyBuilder = new TopologyBuilder();

    topologyBuilder.setSpout("EventHubsSpout", eventHubSpout, spoutConfig.getPartitionCount())
      .setNumTasks(spoutConfig.getPartitionCount());
    topologyBuilder.setBolt("PartialCountBolt", new PartialCountBolt(), spoutConfig.getPartitionCount())
      .localOrShuffleGrouping("EventHubsSpout").setNumTasks(spoutConfig.getPartitionCount());
    topologyBuilder.setBolt("GlobalCountBolt", new GlobalCountBolt(), 1)
      .globalGrouping("PartialCountBolt").setNumTasks(1);
    return topologyBuilder.createTopology();
  }
	
  protected void submitTopology(String[] args, StormTopology topology) throws Exception {
	  Config config = new Config();
    config.setDebug(false);
    //Enable metrics
    config.registerMetricsConsumer(org.apache.storm.metric.LoggingMetricsConsumer.class, 1);

    
	  if (args != null && args.length > 0) {
      config.setNumWorkers(numWorkers);
      StormSubmitter.submitTopology(args[0], config, topology);
    } else {
      config.setMaxTaskParallelism(2);

      LocalCluster localCluster = new LocalCluster();
      localCluster.submitTopology("test", config, topology);

      Thread.sleep(5000000);

      localCluster.shutdown();
    }
	}
  
  protected void runScenario(String[] args) throws Exception{
    readEHConfig(args);
    EventHubSpout eventHubSpout = createEventHubSpout();
    StormTopology topology = buildTopology(eventHubSpout);
    submitTopology(args, topology);
  }

  public static void main(String[] args) throws Exception {
    EventCount scenario = new EventCount();
    scenario.runScenario(args);
  }
}
