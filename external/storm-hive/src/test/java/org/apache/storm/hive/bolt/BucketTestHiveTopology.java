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

package org.apache.storm.hive.bolt;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.MockTupleHelpers;

import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper;
import org.apache.storm.hive.common.HiveOptions;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;


public class BucketTestHiveTopology {
    static final String USER_SPOUT_ID = "user-spout";
    static final String BOLT_ID = "my-hive-bolt";
    static final String TOPOLOGY_NAME = "hive-test-topology1";

    public static void main(String[] args) throws Exception {
        if ((args == null) || (args.length < 7)) {
            System.out.println("Usage: BucketTestHiveTopology metastoreURI "
                    + "dbName tableName dataFileLocation hiveBatchSize " +
                    "hiveTickTupl]eIntervalSecs workers  [topologyNamey] [keytab file]"
                    + " [principal name] ");
            System.exit(1);
        }
        String metaStoreURI = args[0];
        String dbName = args[1];
        String tblName = args[2];
        String sourceFileLocation = args[3];
        Integer hiveBatchSize = Integer.parseInt(args[4]);
        Integer hiveTickTupleIntervalSecs = Integer.parseInt(args[5]);
        Integer workers = Integer.parseInt(args[6]);
        String[] colNames = { "ss_sold_date_sk", "ss_sold_time_sk", "ss_item_sk",
                "ss_customer_sk", "ss_cdemo_sk", "ss_hdemo_sk", "ss_addr_sk",
                "ss_store_sk", "ss_promo_sk", "ss_ticket_number", "ss_quantity",
                "ss_wholesale_cost", "ss_list_price", "ss_sales_price",
                "ss_ext_discount_amt", "ss_ext_sales_price",
                "ss_ext_wholesale_cost", "ss_ext_list_price", "ss_ext_tax",
                "ss_coupon_amt", "ss_net_paid", "ss_net_paid_inc_tax",
                "ss_net_profit" };
        Config config = new Config();
        config.setNumWorkers(workers);
        UserDataSpout spout = new UserDataSpout().withDataFile(sourceFileLocation);
        DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper()
                .withColumnFields(new Fields(colNames)).withTimeAsPartitionField("yyyy/MM/dd");
        HiveOptions hiveOptions;
        hiveOptions = new HiveOptions(metaStoreURI,dbName,tblName,mapper)
            .withTxnsPerBatch(10)
            .withBatchSize(hiveBatchSize);
        // doing below because its affecting storm metrics most likely
        // had to make tick tuple a mandatory argument since its positional
        if (hiveTickTupleIntervalSecs > 0) {
            hiveOptions.withTickTupleInterval(hiveTickTupleIntervalSecs);
        }
        if (args.length == 10) {
            hiveOptions.withKerberosKeytab(args[8]).withKerberosPrincipal(args[9]);
        }
        HiveBolt hiveBolt = new HiveBolt(hiveOptions);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(USER_SPOUT_ID, spout, 1);
        // SentenceSpout --> MyBolt
        builder.setBolt(BOLT_ID, hiveBolt, 14)
                .shuffleGrouping(USER_SPOUT_ID);
        if (args.length == 6) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
            waitForSeconds(20);
            cluster.killTopology(TOPOLOGY_NAME);
            System.out.println("cluster begin to shutdown");
            cluster.shutdown();
            System.out.println("cluster shutdown");
            System.exit(0);
        } else {
            StormSubmitter.submitTopology(args[7], config, builder.createTopology());
        }
    }

    public static void waitForSeconds(int seconds) {
        try {
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException e) {
        }
    }

    public static class UserDataSpout extends BaseRichSpout {
        private ConcurrentHashMap<UUID, Values> pending;
        private SpoutOutputCollector collector;
        private String filePath;
        private BufferedReader br;
        private int count = 0;
        private long total = 0L;
        private String[] outputFields = { "ss_sold_date_sk", "ss_sold_time_sk",
                "ss_item_sk", "ss_customer_sk", "ss_cdemo_sk", "ss_hdemo_sk",
                "ss_addr_sk", "ss_store_sk", "ss_promo_sk", "ss_ticket_number",
                "ss_quantity", "ss_wholesale_cost", "ss_list_price",
                "ss_sales_price", "ss_ext_discount_amt", "ss_ext_sales_price",
                "ss_ext_wholesale_cost", "ss_ext_list_price", "ss_ext_tax",
                "ss_coupon_amt", "ss_net_paid", "ss_net_paid_inc_tax",
                "ss_net_profit" };

        public UserDataSpout withDataFile (String filePath) {
            this.filePath = filePath;
            return this;
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(this.outputFields));
        }

        public void open(Map config, TopologyContext context,
                         SpoutOutputCollector collector) {
            this.collector = collector;
            this.pending = new ConcurrentHashMap<UUID, Values>();
            try {
                this.br = new BufferedReader(new FileReader(new File(this
                        .filePath)));
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        public void nextTuple() {
            String line;
            try {
                if ((line = br.readLine()) != null) {
                    System.out.println("*********" + line);
                    String[] values = line.split("\\|", -1);
                    // above gives an extra empty string at the end. below
                    // removes that
                    values = Arrays.copyOfRange(values, 0,
                            this.outputFields.length);
                    Values tupleValues = new Values(values);
                    UUID msgId = UUID.randomUUID();
                    this.pending.put(msgId, tupleValues);
                    this.collector.emit(tupleValues, msgId);
                    count++;
                    total++;
                    if (count > 1000) {
                        count = 0;
                        System.out.println("Pending count: " + this.pending.size() + ", total: " + this.total);
                    }
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        public void ack(Object msgId) {
            this.pending.remove(msgId);
        }

        public void fail(Object msgId) {
            System.out.println("**** RESENDING FAILED TUPLE");
            this.collector.emit(this.pending.get(msgId), msgId);
        }
    }
}
