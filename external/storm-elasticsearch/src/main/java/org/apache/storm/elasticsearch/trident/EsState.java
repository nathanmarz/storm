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

import backtype.storm.task.IMetricsContext;
import backtype.storm.topology.FailedException;
import org.apache.storm.elasticsearch.common.EsConfig;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.TridentCollector;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EsState implements State {
    private static final Logger LOG = LoggerFactory.getLogger(EsState.class);
    private static Client client;
    private EsConfig esConfig;

    /**
     * EsState constructor
     * @param esConfig Elasticsearch configuration containing node addresses and cluster name {@link EsConfig}
     */
    public EsState(EsConfig esConfig) {
        this.esConfig = esConfig;
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

    public void prepare(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        try {
            synchronized (EsState.class) {
                if (client == null) {
                    Settings settings =
                            ImmutableSettings.settingsBuilder().put("cluster.name", esConfig.getClusterName())
                                    .put("client.transport.sniff", "true").build();
                    List<InetSocketTransportAddress> transportAddressList = new ArrayList<InetSocketTransportAddress>();
                    for (String node : esConfig.getNodes()) {
                        String[] hostAndPort = node.split(":");
                        if (hostAndPort.length != 2) {
                            throw new IllegalArgumentException("incorrect Elasticsearch node format, should follow {host}:{port} pattern");
                        }
                        transportAddressList.add(new InetSocketTransportAddress(hostAndPort[0], Integer.parseInt(hostAndPort[1])));
                    }
                    client = new TransportClient(settings)
                            .addTransportAddresses(transportAddressList.toArray(new InetSocketTransportAddress[transportAddressList.size()]));
                }
            }
        } catch (Exception e) {
            LOG.warn("unable to initialize EsState ", e);
        }
    }

    public void updateState(List<TridentTuple> tuples, TridentCollector collector) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (TridentTuple tuple : tuples) {
            String source = tuple.getStringByField("source");
            String index = tuple.getStringByField("index");
            String type = tuple.getStringByField("type");
            String id = tuple.getStringByField("id");

            bulkRequest.add(client.prepareIndex(index, type, id).setSource(source));
        }
        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()) {
            LOG.warn("failed processing bulk index requests " + bulkResponse.buildFailureMessage());
            throw new FailedException();
        }
    }
}
