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
package org.apache.storm.elasticsearch.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.elasticsearch.common.EsConfig;
import org.apache.storm.elasticsearch.common.EsConstants;
import org.apache.storm.elasticsearch.common.EsTestUtil;
import org.apache.storm.elasticsearch.common.EsTupleMapper;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.trident.state.StateFactory;

import java.util.*;

public class TridentEsTopology {

    static final String TOPOLOGY_NAME = "elasticsearch-test-topology2";

    public static void main(String[] args) {
        int batchSize = 100;
        FixedBatchSpout spout = new FixedBatchSpout(batchSize);
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newStream("spout", spout);
        EsConfig esConfig = new EsConfig(EsConstants.clusterName, new String[]{"localhost:9300"});
        Fields esFields = new Fields("index", "type", "source");
        EsTupleMapper tupleMapper = EsTestUtil.generateDefaultTupleMapper();
        StateFactory factory = new EsStateFactory(esConfig, tupleMapper);
        TridentState state = stream.partitionPersist(factory, esFields, new EsUpdater(), new Fields());

        EsTestUtil.startEsNode();
        EsTestUtil.waitForSeconds(5);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, null, topology.build());
        EsTestUtil.waitForSeconds(20);
        cluster.killTopology(TOPOLOGY_NAME);
        System.out.println("cluster begin to shutdown");
        cluster.shutdown();
        System.out.println("cluster shutdown");
        System.exit(0);
    }

    public static class FixedBatchSpout implements IBatchSpout {
        int maxBatchSize;
        HashMap<Long, List<List<Object>>> batches = new HashMap<Long, List<List<Object>>>();
        private Values[] outputs = {
                new Values("{\"user\":\"user1\"}", "index1", "type1", UUID.randomUUID().toString()),
                new Values("{\"user\":\"user2\"}", "index1", "type2", UUID.randomUUID().toString()),
                new Values("{\"user\":\"user3\"}", "index2", "type1", UUID.randomUUID().toString()),
                new Values("{\"user\":\"user4\"}", "index2", "type2", UUID.randomUUID().toString())
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
            return new Fields("source", "index", "type", "id");
        }

        @Override
        public void open(Map conf, TopologyContext context) {
            index = 0;
        }

        @Override
        public void emitBatch(long batchId, TridentCollector collector) {
            List<List<Object>> batch = this.batches.get(batchId);
            if (batch == null) {
                batch = new ArrayList<List<Object>>();
                if (index >= outputs.length && cycle) {
                    index = 0;
                }
                for (int i = 0; i < maxBatchSize; index++, i++) {
                    if (index == outputs.length) {
                        index = 0;
                    }
                    batch.add(outputs[index]);
                }
                this.batches.put(batchId, batch);
            }
            for (List<Object> list : batch) {
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
        public Map<String, Object> getComponentConfiguration() {
            Config conf = new Config();
            conf.setMaxTaskParallelism(1);
            return conf;
        }
    }
}
