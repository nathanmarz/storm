/**
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
 */
package org.apache.storm.starter.bolt;

import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.log4j.Logger;
import org.apache.storm.starter.tools.NthLastModifiedTimeTracker;
import org.apache.storm.starter.tools.SlidingWindowCounter;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * This bolt aggregates counts from multiple upstream bolts.
 */
public class RollingCountAggBolt extends BaseRichBolt {
  private static final long serialVersionUID = 5537727428628598519L;
  private static final Logger LOG = Logger.getLogger(RollingCountAggBolt.class);
  //Mapping of key->upstreamBolt->count
  private Map<Object, Map<Integer, Long>> counts = new HashMap<Object, Map<Integer, Long>>();
  private OutputCollector collector;


  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
  }

  @Override
  public void execute(Tuple tuple) {
    Object obj = tuple.getValue(0);
    long count = tuple.getLong(1);
    int source = tuple.getSourceTask();
    Map<Integer, Long> subCounts = counts.get(obj);
    if (subCounts == null) {
      subCounts = new HashMap<Integer, Long>();
      counts.put(obj, subCounts);
    }
    //Update the current count for this object
    subCounts.put(source, count);
    //Output the sum of all the known counts so for this key
    long sum = 0;
    for (Long val: subCounts.values()) {
      sum += val;
    }
    collector.emit(new Values(obj, sum));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("obj", "count"));
  }
}
