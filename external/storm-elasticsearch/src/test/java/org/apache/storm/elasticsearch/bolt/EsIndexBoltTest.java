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

import org.apache.storm.testing.IntegrationTest;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.elasticsearch.common.EsConfig;
import org.apache.storm.elasticsearch.common.EsTestUtil;
import org.apache.storm.elasticsearch.common.EsTupleMapper;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.mockito.Mockito.verify;

@Category(IntegrationTest.class)
public class EsIndexBoltTest extends AbstractEsBoltIntegrationTest<EsIndexBolt> {

    @Test
    public void testEsIndexBolt()
            throws Exception {
        String index = "index1";
        String type = "type1";

        Tuple tuple = createTestTuple(index, type);

        bolt.execute(tuple);

        verify(outputCollector).ack(tuple);

        node.client().admin().indices().prepareRefresh(index).execute().actionGet();
        CountResponse resp = node.client().prepareCount(index)
                .setQuery(new TermQueryBuilder("_type", type))
                .execute().actionGet();

        Assert.assertEquals(1, resp.getCount());
    }

    private Tuple createTestTuple(String index, String type) {
        String source = "{\"user\":\"user1\"}";
        String id = "docId";
        return EsTestUtil.generateTestTuple(source, index, type, id);
    }

    @Override
    protected EsIndexBolt createBolt(EsConfig esConfig) {
        EsTupleMapper tupleMapper = EsTestUtil.generateDefaultTupleMapper();
        return new EsIndexBolt(esConfig, tupleMapper);
    }

    @Override
    protected Class<EsIndexBolt> getBoltClass() {
        return EsIndexBolt.class;
    }
}
