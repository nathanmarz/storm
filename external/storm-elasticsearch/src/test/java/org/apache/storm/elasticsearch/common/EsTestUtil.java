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
package org.apache.storm.elasticsearch.common;

import backtype.storm.Config;
import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.tuple.Values;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.util.HashMap;

public class EsTestUtil {
    public static Tuple generateTestTuple(String source, String index, String type, String id) {
        TopologyBuilder builder = new TopologyBuilder();
        GeneralTopologyContext topologyContext = new GeneralTopologyContext(builder.createTopology(),
                new Config(), new HashMap(), new HashMap(), new HashMap(), "") {
            @Override
            public Fields getComponentOutputFields(String componentId, String streamId) {
                return new Fields("source", "index", "type", "id");
            }
        };
        return new TupleImpl(topologyContext, new Values(source, index, type, id), 1, "");
    }

    public static Node startEsNode(){
        Node node = NodeBuilder.nodeBuilder().data(true).settings(
                ImmutableSettings.builder()
                        .put(ClusterName.SETTING, EsConstants.clusterName)
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(EsExecutors.PROCESSORS, 1)
                        .put("http.enabled", false)
                        .put("index.percolator.map_unmapped_fields_as_string", true)
                        .put("index.store.type", "memory")
        ).build();
        node.start();
        return node;
    }

    public static void waitForSeconds(int seconds) {
        try {
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException e) {
        }
    }
}
