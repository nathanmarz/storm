/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.elasticsearch.trident;

import org.apache.storm.topology.FailedException;

import org.apache.storm.elasticsearch.common.StormElasticSearchClient;
import org.apache.storm.elasticsearch.common.EsConfig;
import org.apache.storm.elasticsearch.common.EsTupleMapper;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.List;

/**
 * Trident State for storing tuple to ES document.
 * @since 0.11
 */
class EsState implements State {
    private static final Logger LOG = LoggerFactory.getLogger(EsState.class);
    private static Client client;
    private EsConfig esConfig;
    private EsTupleMapper tupleMapper;

    /**
     * EsState constructor
     * @param esConfig Elasticsearch configuration containing node addresses and cluster name {@link EsConfig}
     * @param tupleMapper Tuple to ES document mapper {@link EsTupleMapper}
     */
    public EsState(EsConfig esConfig, EsTupleMapper tupleMapper) {
        this.esConfig = esConfig;
        this.tupleMapper = tupleMapper;
    }

    /**
     * @param txid
     *
     * Elasticsearch index requests with same id will result in update operation
     * which means if same tuple replays, only one record will be stored in elasticsearch for same document
     * without control with txid
     */
    @Override
    public void beginCommit(Long txid) {

    }

    /**
     * @param txid
     *
     * Elasticsearch index requests with same id will result in update operation
     * which means if same tuple replays, only one record will be stored in elasticsearch for same document
     * without control with txid
     */
    @Override
    public void commit(Long txid) {

    }

    public void prepare() {
        try {
            synchronized (EsState.class) {
                if (client == null) {
                    client = new StormElasticSearchClient(esConfig).construct();
                }
            }
        } catch (Exception e) {
            LOG.warn("unable to initialize EsState ", e);
        }
    }

    /**
     * Store current state to ElasticSearch.
     *
     * @param tuples list of tuples for storing to ES.
     *               Each tuple should have relevant fields (source, index, type, id) for EsState's tupleMapper to extract ES document.
     */
    public void updateState(List<TridentTuple> tuples) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (TridentTuple tuple : tuples) {
            String source = tupleMapper.getSource(tuple);
            String index = tupleMapper.getIndex(tuple);
            String type = tupleMapper.getType(tuple);
            String id = tupleMapper.getId(tuple);

            bulkRequest.add(client.prepareIndex(index, type, id).setSource(source));
        }
        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()) {
            LOG.warn("failed processing bulk index requests " + bulkResponse.buildFailureMessage());
            throw new FailedException();
        }
    }
}
