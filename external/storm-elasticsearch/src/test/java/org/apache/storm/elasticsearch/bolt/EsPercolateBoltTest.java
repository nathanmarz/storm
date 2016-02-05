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
import org.apache.storm.tuple.Values;
import org.apache.storm.elasticsearch.common.EsConfig;
import org.apache.storm.elasticsearch.common.EsTestUtil;
import org.apache.storm.elasticsearch.common.EsTupleMapper;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;

@Category(IntegrationTest.class)
public class EsPercolateBoltTest extends AbstractEsBoltIntegrationTest<EsPercolateBolt> {

    @Override
    protected EsPercolateBolt createBolt(EsConfig esConfig) {
        EsTupleMapper tupleMapper = EsTestUtil.generateDefaultTupleMapper();
        return new EsPercolateBolt(esConfig, tupleMapper);
    }

    @Test
    public void testEsPercolateBolt()
            throws Exception {
        String source = "{\"user\":\"user1\"}";
        String index = "index1";
        String type = ".percolator";

        node.client().prepareIndex("index1", ".percolator")
                .setId("1")
                .setSource("{\"query\":{\"match\":{\"user\":\"user1\"}}}").
                execute().actionGet();
        Tuple tuple = EsTestUtil.generateTestTuple(source, index, type, null);

        bolt.execute(tuple);

        verify(outputCollector).ack(tuple);
        verify(outputCollector).emit(new Values(source, any(PercolateResponse.Match.class)));
    }

    @Override
    protected Class<EsPercolateBolt> getBoltClass() {
        return EsPercolateBolt.class;
    }
}
