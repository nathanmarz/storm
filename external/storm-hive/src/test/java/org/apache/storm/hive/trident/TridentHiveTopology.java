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

package org.apache.storm.hive.trident;


import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper;
import org.apache.storm.hive.common.HiveOptions;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.task.TopologyContext;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;


public class TridentHiveTopology {
    public static StormTopology buildTopology(String metaStoreURI, String dbName, String tblName, Object keytab, Object principal) {
        int batchSize = 100;
        FixedBatchSpout spout = new FixedBatchSpout(batchSize);
        spout.setCycle(true);
        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newStream("hiveTridentspout1",spout);
        String[] partNames = {"city","state"};
        String[] colNames = {"id","name","phone","street"};
        Fields hiveFields = new Fields("id","name","phone","street","city","state");
        DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper()
            .withColumnFields(new Fields(colNames))
            .withPartitionFields(new Fields(partNames));
        HiveOptions hiveOptions;
        if (keytab != null && principal != null) {
            hiveOptions = new HiveOptions(metaStoreURI,dbName,tblName,mapper)
                .withTxnsPerBatch(10)
                .withBatchSize(batchSize)
                .withIdleTimeout(10)
                .withCallTimeout(30000)
                .withKerberosKeytab((String)keytab)
                .withKerberosPrincipal((String)principal);
        } else  {
            hiveOptions = new HiveOptions(metaStoreURI,dbName,tblName,mapper)
                .withTxnsPerBatch(10)
                .withBatchSize(batchSize)
                .withCallTimeout(30000)
                .withIdleTimeout(10);
        }
        StateFactory factory = new HiveStateFactory().withOptions(hiveOptions);
        TridentState state = stream.partitionPersist(factory, hiveFields, new HiveUpdater(), new Fields());
        return topology.build();
    }

    public static void waitForSeconds(int seconds) {
        try {
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException e) {
        }
    }

    public static void main(String[] args) {
        String metaStoreURI = args[0];
        String dbName = args[1];
        String tblName = args[2];
        Config conf = new Config();
        conf.setMaxSpoutPending(5);
        if(args.length == 3) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("tridentHiveTopology", conf, buildTopology(metaStoreURI, dbName, tblName,null,null));
            System.out.println("waiting for 60 seconds");
            waitForSeconds(60);
            System.out.println("killing topology");
            cluster.killTopology("tridenHiveTopology");
            System.out.println("cluster shutdown");
            cluster.shutdown();
            System.out.println("cluster shutdown");
            System.exit(0);
        } else if(args.length == 4) {
            try {
                StormSubmitter.submitTopology(args[3], conf, buildTopology(metaStoreURI, dbName, tblName,null,null));
            } catch(Exception e) {
                System.out.println("Failed to submit topology "+e);
            }
        } else if (args.length == 6) {
            try {
                StormSubmitter.submitTopology(args[3], conf, buildTopology(metaStoreURI, dbName, tblName,args[4],args[5]));
            } catch(Exception e) {
                System.out.println("Failed to submit topology "+e);
            }
        } else {
            System.out.println("Usage: TridentHiveTopology metastoreURI dbName tableName [topologyNamey]");
        }
    }

    public static class FixedBatchSpout implements IBatchSpout {
        int maxBatchSize;
        HashMap<Long, List<List<Object>>> batches = new HashMap<Long, List<List<Object>>>();
        private Values[] outputs = {
            new Values("1","user1","123456","street1","sunnyvale","ca"),
            new Values("2","user2","123456","street2","sunnyvale","ca"),
            new Values("3","user3","123456","street3","san jose","ca"),
            new Values("4","user4","123456","street4","san jose","ca"),
        };
        private int index = 0;
        boolean cycle = false;

        public FixedBatchSpout(int maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
        }

        public void setCycle(boolean cycle) {
            this.cycle = cycle;
        }

        @Override
        public Fields getOutputFields() {
            return new Fields("id","name","phone","street","city","state");
        }

        @Override
        public void open(Map conf, TopologyContext context) {
            index = 0;
        }

        @Override
        public void emitBatch(long batchId, TridentCollector collector) {
            List<List<Object>> batch = this.batches.get(batchId);
            if(batch == null){
                batch = new ArrayList<List<Object>>();
                if(index>=outputs.length && cycle) {
                    index = 0;
                }
                for(int i=0; i < maxBatchSize; index++, i++) {
                    if(index == outputs.length){
                        index=0;
                    }
                    batch.add(outputs[index]);
                }
                this.batches.put(batchId, batch);
            }
            for(List<Object> list : batch){
                collector.emit(list);
            }
        }

        @Override
        public void ack(long batchId) {
            this.batches.remove(batchId);
        }

        @Override
        public void close() {
        }

        @Override
        public Map getComponentConfiguration() {
            Config conf = new Config();
            conf.setMaxTaskParallelism(1);
            return conf;
        }

    }

}
