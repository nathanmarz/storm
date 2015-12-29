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

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.Nimbus;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.spout.Configs;
import org.apache.storm.hdfs.spout.HdfsSpout;

import java.util.Map;


public class HdfsSpoutTopology {

  public static final String SPOUT_ID = "hdfsspout";
  public static final String BOLT_ID = "hdfsbolt";

  public static final int SPOUT_NUM = 4;
  public static final int BOLT_NUM = 4;
  public static final int WORKER_NUM = 4;


  private static HdfsBolt makeHdfsBolt(String arg, String destinationDir) {
    DefaultFileNameFormat fileNameFormat = new DefaultFileNameFormat()
            .withPath(destinationDir)
            .withExtension(".txt");
    RecordFormat format = new DelimitedRecordFormat();
    FileRotationPolicy rotationPolicy = new TimedRotationPolicy(5.0f, TimedRotationPolicy.TimeUnit.MINUTES);

    return new HdfsBolt()
            .withConfigKey("hdfs.config")
            .withFsUrl(arg)
            .withFileNameFormat(fileNameFormat)
            .withRecordFormat(format)
            .withRotationPolicy(rotationPolicy)
            .withSyncPolicy(new CountSyncPolicy(1000));
  }

  /** Copies text file content from sourceDir to destinationDir. Moves source files into sourceDir after its done consuming
   *    args: sourceDir sourceArchiveDir badDir destinationDir
   */
  public static void main(String[] args) throws Exception {
    // 0 - validate args
    if (args.length < 6) {
      System.err.println("Please check command line arguments.");
      System.err.println("Usage :");
      System.err.println(HdfsSpoutTopology.class.toString() + " topologyName fileFormat sourceDir sourceArchiveDir badDir destinationDir.");
      System.err.println(" topologyName - topology name.");
      System.err.println(" fileFormat -  Set to 'TEXT' for reading text files or 'SEQ' for sequence files.");
      System.err.println(" sourceDir  - read files from this HDFS dir using HdfsSpout.");
      System.err.println(" sourceArchiveDir - after a file in sourceDir is read completely, it is moved to this HDFS location.");
      System.err.println(" badDir - files that cannot be read properly will be moved to this HDFS location.");
      System.err.println(" destinationDir - write data out to this HDFS location using HDFS bolt.");

      System.err.println();
      System.exit(-1);
    }

    // 1 - parse cmd line args
    String topologyName = args[0];
    String fileFormat = args[1];
    String sourceDir = args[2];
    String sourceArchiveDir = args[3];
    String badDir = args[4];
    String destinationDir = args[5];

    // 2 - create and configure spout and bolt
    HdfsBolt bolt = makeHdfsBolt(args[0], destinationDir);
    HdfsSpout spout = new HdfsSpout().withOutputFields("line");

    Config conf = new Config();
    conf.put(Configs.SOURCE_DIR, sourceDir);
    conf.put(Configs.ARCHIVE_DIR, sourceArchiveDir);
    conf.put(Configs.BAD_DIR, badDir);
    conf.put(Configs.READER_TYPE, fileFormat);

    // 3 - Create and configure topology
    conf.setDebug(true);
    conf.setNumWorkers(WORKER_NUM);
    conf.registerMetricsConsumer(backtype.storm.metric.LoggingMetricsConsumer.class);

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout(SPOUT_ID, spout, SPOUT_NUM);
    builder.setBolt(BOLT_ID, bolt, BOLT_NUM).shuffleGrouping(SPOUT_ID);

    // 4 - submit topology, wait for few min and terminate it
    Map clusterConf = Utils.readStormConfig();
    StormSubmitter.submitTopologyWithProgressBar(topologyName, conf, builder.createTopology());
    Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();

    // 5 - Print metrics every 30 sec, kill topology after 5 min
    for (int i = 0; i < 10; i++) {
      Thread.sleep(30 * 1000);
      FastWordCountTopology.printMetrics(client, topologyName);
    }
    FastWordCountTopology.kill(client, topologyName);
  } // main

}
