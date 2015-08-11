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

import backtype.storm.tuple.Tuple;
import org.apache.storm.elasticsearch.common.EsConfig;
import org.apache.storm.elasticsearch.common.EsTestUtil;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.mockito.Mockito.verify;

public class EsIndexBoltTest extends AbstractEsBoltTest{
    private static final Logger LOG = LoggerFactory.getLogger(EsIndexBoltTest.class);
    private EsIndexBolt bolt;

    @Test
    public void testEsIndexBolt()
            throws Exception {
        EsConfig esConfig = new EsConfig();
        esConfig.setClusterName("test-cluster");
        esConfig.setNodes(new String[]{"127.0.0.1:9300"});

        bolt = new EsIndexBolt(esConfig);
        bolt.prepare(config, null, collector);

        String source = "{\"user\":\"user1\"}";
        String index = "index1";
        String type = "type1";
        String id = "docId";
        Tuple tuple = EsTestUtil.generateTestTuple(source, index, type, id);

        bolt.execute(tuple);

        verify(collector).ack(tuple);

        node.client().admin().indices().prepareRefresh(index).execute().actionGet();
        CountResponse resp = node.client().prepareCount(index)
                .setQuery(new TermQueryBuilder("_type", type))
                .execute().actionGet();

        Assert.assertEquals(1, resp.getCount());

        bolt.cleanup();
    }
}