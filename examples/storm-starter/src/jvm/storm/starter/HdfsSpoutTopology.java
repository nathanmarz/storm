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

package storm.starter;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.metric.LoggingMetricsConsumer;
import org.apache.storm.starter.FastWordCountTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.apache.storm.hdfs.spout.Configs;
import org.apache.storm.hdfs.spout.HdfsSpout;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.*;
import org.apache.storm.tuple.*;
import org.apache.storm.task.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class HdfsSpoutTopology {

  public static final String SPOUT_ID = "hdfsspout";
  public static final String BOLT_ID = "constbolt";

  public static final int WORKER_NUM = 1;

  public static class ConstBolt extends BaseRichBolt {
    private static final long serialVersionUID = -5313598399155365865L;
    public static final String FIELDS = "message";
    private OutputCollector collector;
    private static final Logger log = LoggerFactory.getLogger(ConstBolt.class);
    int count =0;

    public ConstBolt() {
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
      log.info("Received tuple : {}", tuple.getValue(0));
      count++;
      if(count==3) {
        collector.fail(tuple);
      }
      else {
        collector.ack(tuple);
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields(FIELDS));
    }
  } // class

  /** Copies text file content from sourceDir to destinationDir. Moves source files into sourceDir after its done consuming */
  public static void main(String[] args) throws Exception {
    // 0 - validate args
    if (args.length < 7) {
      System.err.println("Please check command line arguments.");
      System.err.println("Usage :");
      System.err.println(HdfsSpoutTopology.class.toString() + " topologyName hdfsUri fileFormat sourceDir sourceArchiveDir badDir destinationDir.");
      System.err.println(" topologyName - topology name.");
      System.err.println(" hdfsUri - hdfs name node URI");
      System.err.println(" fileFormat -  Set to 'TEXT' for reading text files or 'SEQ' for sequence files.");
      System.err.println(" sourceDir  - read files from this HDFS dir using HdfsSpout.");
      System.err.println(" archiveDir - after a file in sourceDir is read completely, it is moved to this HDFS location.");
      System.err.println(" badDir - files that cannot be read properly will be moved to this HDFS location.");
      System.err.println(" spoutCount - Num of spout instances.");
      System.err.println();
      System.exit(-1);
    }

    // 1 - parse cmd line args
    String topologyName = args[0];
    String hdfsUri = args[1];
    String fileFormat = args[2];
    String sourceDir = args[3];
    String sourceArchiveDir = args[4];
    String badDir = args[5];
    int spoutNum = Integer.parseInt(args[6]);

    // 2 - create and configure spout and bolt
    ConstBolt bolt = new ConstBolt();

    HdfsSpout spout = new HdfsSpout().withOutputFields("line");
    Config conf = new Config();
    conf.put(Configs.SOURCE_DIR, sourceDir);
    conf.put(Configs.ARCHIVE_DIR, sourceArchiveDir);
    conf.put(Configs.BAD_DIR, badDir);
    conf.put(Configs.READER_TYPE, fileFormat);
    conf.put(Configs.HDFS_URI, hdfsUri);
    conf.setDebug(true);
    conf.setNumWorkers(1);
    conf.setNumAckers(1);
    conf.setMaxTaskParallelism(1);

    // 3 - Create and configure topology
    conf.setDebug(true);
    conf.setNumWorkers(WORKER_NUM);
    conf.registerMetricsConsumer(LoggingMetricsConsumer.class);

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout(SPOUT_ID, spout, spoutNum);
    builder.setBolt(BOLT_ID, bolt, 1).shuffleGrouping(SPOUT_ID);

    // 4 - submit topology, wait for a few min and terminate it
    Map clusterConf = Utils.readStormConfig();
    StormSubmitter.submitTopologyWithProgressBar(topologyName, conf, builder.createTopology());
    Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();

    // 5 - Print metrics every 30 sec, kill topology after 20 min
    for (int i = 0; i < 40; i++) {
      Thread.sleep(30 * 1000);
      FastWordCountTopology.printMetrics(client, topologyName);
    }
    FastWordCountTopology.kill(client, topologyName);
  } // main

}
