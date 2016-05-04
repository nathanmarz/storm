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
package org.apache.storm.starter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * This is a basic example of a Storm topology.
 */
public class MultipleLoggerTopology {
  public static class ExclamationLoggingBolt extends BaseRichBolt {
    OutputCollector _collector;
    Logger _rootLogger = LoggerFactory.getLogger (Logger.ROOT_LOGGER_NAME);
    // ensure the loggers are configured in the worker.xml before
    // trying to use them here
    Logger _logger = LoggerFactory.getLogger ("com.myapp");
    Logger _subLogger = LoggerFactory.getLogger ("com.myapp.sub");

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
      _rootLogger.debug ("root: This is a DEBUG message");
      _rootLogger.info ("root: This is an INFO message");
      _rootLogger.warn ("root: This is a WARN message");
      _rootLogger.error ("root: This is an ERROR message");

      _logger.debug ("myapp: This is a DEBUG message");
      _logger.info ("myapp: This is an INFO message");
      _logger.warn ("myapp: This is a WARN message");
      _logger.error ("myapp: This is an ERROR message");

      _subLogger.debug ("myapp.sub: This is a DEBUG message");
      _subLogger.info ("myapp.sub: This is an INFO message");
      _subLogger.warn ("myapp.sub: This is a WARN message");
      _subLogger.error ("myapp.sub: This is an ERROR message");

      _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
      _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }
  }

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("word", new TestWordSpout(), 10);
    builder.setBolt("exclaim1", new ExclamationLoggingBolt(), 3).shuffleGrouping("word");
    builder.setBolt("exclaim2", new ExclamationLoggingBolt(), 2).shuffleGrouping("exclaim1");

    Config conf = new Config();
    conf.setDebug(true);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(2);
      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    } else {
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("test", conf, builder.createTopology());
      Utils.sleep(10000);
      cluster.killTopology("test");
      cluster.shutdown();
    }
  }
}
