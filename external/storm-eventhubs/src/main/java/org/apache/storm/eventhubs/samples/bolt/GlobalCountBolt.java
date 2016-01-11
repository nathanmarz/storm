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
package org.apache.storm.eventhubs.samples.bolt;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.Config;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

/**
 * Globally count number of messages
 */
public class GlobalCountBolt extends BaseBasicBolt {
  private static final long serialVersionUID = 1L;
  private static final Logger logger = LoggerFactory
      .getLogger(GlobalCountBolt.class);
  private long globalCount;
  private long globalCountDiff;
  private long lastMetricsTime;
  private long throughput;
  
  @Override
  public void prepare(Map config, TopologyContext context) {
    globalCount = 0;
    globalCountDiff = 0;
    lastMetricsTime = System.nanoTime();
    context.registerMetric("GlobalMessageCount", new IMetric() {
      @Override
      public Object getValueAndReset() {
        long now = System.nanoTime();
        long millis = (now - lastMetricsTime) / 1000000;
        throughput = globalCountDiff / millis * 1000;
        Map values = new HashMap();
        values.put("global_count", globalCount);
        values.put("throughput", throughput);
        lastMetricsTime = now;
        globalCountDiff = 0;
        return values;
      }
  }, (Integer)config.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS));
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    int partial = (Integer)tuple.getValueByField("partial_count");
    globalCount += partial;
    globalCountDiff += partial;
    if((globalCountDiff == partial) && (globalCount != globalCountDiff)) {
      //metrics has just been collected, let's also log it
      logger.info("Current throughput (messages/second): " + throughput);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    
  }

}
