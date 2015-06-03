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
package org.apache.storm.elasticsearch.bolt;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.Before;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

public class AbstractEsBoltTest {
    protected Config config = new Config();
    protected OutputCollector collector = mock(OutputCollector.class);
    protected Node node;

    @Before
    public void setup() throws Exception {
        System.out.println("setup");
        node = NodeBuilder.nodeBuilder().data(true).settings(
                ImmutableSettings.builder()
                        .put(ClusterName.SETTING, "test-cluster")
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(EsExecutors.PROCESSORS, 1)
                        .put("http.enabled", false)
                        .put("index.percolator.map_unmapped_fields_as_string", true)
                        .put("index.store.type", "memory")
        ).build();
        node.start();
        ensureEsGreen(node);
        ClusterHealthResponse chr = node.client().admin().cluster()
                .health(Requests.clusterHealthRequest().timeout(TimeValue.timeValueSeconds(30)).waitForGreenStatus().waitForRelocatingShards(0)).actionGet();
        System.out.println(chr.getStatus());
        Thread.sleep(1000);
    }

    @After
    public void cleanup() throws Exception {
        System.out.println("cleanup");
        node.stop();
        node.close();
    }

    private void ensureEsGreen(Node node) {
        ClusterHealthResponse chr = node.client().admin().cluster()
                .health(Requests.clusterHealthRequest().timeout(TimeValue.timeValueSeconds(30)).waitForGreenStatus().waitForEvents(Priority.LANGUID).waitForRelocatingShards(0)).actionGet();
        assertThat("cluster status is green", chr.getStatus(), equalTo(ClusterHealthStatus.GREEN));
    }
}
