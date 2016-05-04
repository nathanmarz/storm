/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.elasticsearch.bolt;

import org.apache.storm.testing.IntegrationTest;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.elasticsearch.ElasticsearchGetRequest;
import org.apache.storm.elasticsearch.EsLookupResultOutput;
import org.apache.storm.elasticsearch.common.EsConfig;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;

@Category(IntegrationTest.class)
@RunWith(MockitoJUnitRunner.class)
public class EsLookupBoltIntegrationTest extends AbstractEsBoltIntegrationTest<EsLookupBolt> {

    private final String documentId = UUID.randomUUID().toString();
    private final String indexName = "index";
    private final String typeName = "type";
    private final String source = "{\"user\":\"user1\"}";

    private ElasticsearchGetRequest getRequest = new TestElasticsearchGetRequest();
    private EsLookupResultOutput output = new TestEsLookupResultOutput();

    @Captor
    private ArgumentCaptor<Tuple> anchor;

    @Captor
    private ArgumentCaptor<Values> emmitedValues;

    @Mock
    private Tuple tuple;

    @Override
    protected EsLookupBolt createBolt(EsConfig esConfig) {
        return new EsLookupBolt(esConfig, getRequest, output);
    }

    @Before
    public void populateIndexWithTestData() throws Exception {
        node.client().prepareIndex(indexName, typeName, documentId).setSource(source).execute().actionGet();
    }

    @Before
    public void clearIndex() throws Exception {
        node.client().delete(new DeleteRequest(indexName, typeName, documentId)).actionGet();
    }

    @Test
    public void anchorsTheTuple() throws Exception {
        bolt.execute(tuple);

        verify(outputCollector).emit(anchor.capture(), emmitedValues.capture());
        assertThat(anchor.getValue(), is(tuple));
    }

    @Test
    public void emitsExpectedValues() throws Exception {
        Values expectedValues = expectedValues();

        bolt.execute(tuple);

        verify(outputCollector).emit(anchor.capture(), emmitedValues.capture());
        assertThat(emmitedValues.getValue(), is(expectedValues));
    }

    @Test
    public void acksTuple() throws Exception {
        bolt.execute(tuple);

        verify(outputCollector).ack(anchor.capture());
        assertThat(anchor.getValue(), is(tuple));
    }

    private Values expectedValues() {
        return new Values(source);
    }

    private class TestElasticsearchGetRequest implements ElasticsearchGetRequest {

        @Override
        public GetRequest extractFrom(ITuple tuple) {
            return node.client().prepareGet().setId(documentId).setIndex(indexName).setType(typeName).request();
        }
    }

    private class TestEsLookupResultOutput implements EsLookupResultOutput {

        @Override
        public Collection<Values> toValues(GetResponse response) {
            return Collections.singleton(expectedValues());
        }

        @Override
        public Fields fields() {
            return new Fields("data");
        }
    }

    @Override
    protected Class<EsLookupBolt> getBoltClass() {
        return EsLookupBolt.class;
    }
}
