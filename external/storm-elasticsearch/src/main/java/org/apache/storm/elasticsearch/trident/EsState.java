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
    private EsConfig esConfig;
    private static Client client;
    private static final Logger LOG = LoggerFactory.getLogger(EsState.class);

    public EsState(EsConfig esConfig) {
        this.esConfig = esConfig;
    }

    @Override
    public void beginCommit(Long txid) {

    }

    @Override
    public void commit(Long txid) {

    }

    public void prepare(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        synchronized (EsState.class) {
            if (client == null) {
                Settings settings =
                        ImmutableSettings.settingsBuilder().put("cluster.name", esConfig.getClusterName())
                                .put("client.transport.sniff", "true").build();
                List<InetSocketTransportAddress> transportAddressList = new ArrayList<InetSocketTransportAddress>();
                for (String host : esConfig.getHost()) {
                    transportAddressList.add(new InetSocketTransportAddress(host, esConfig.getPort()));
                }
                client = new TransportClient(settings)
                        .addTransportAddresses(transportAddressList.toArray(new InetSocketTransportAddress[transportAddressList.size()]));
            }
        }

    }

    public void updateState(List<TridentTuple> tuples, TridentCollector collector) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (TridentTuple tuple : tuples) {
            String index = tuple.getStringByField("index");
            String type = tuple.getStringByField("type");
            String source = tuple.getStringByField("source");
            bulkRequest.add(client.prepareIndex(index, type).setSource(source));
        }
        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()) {
            LOG.warn("failed processing bulk index requests " + bulkResponse.buildFailureMessage());
            throw new FailedException();
        }
    }
}
