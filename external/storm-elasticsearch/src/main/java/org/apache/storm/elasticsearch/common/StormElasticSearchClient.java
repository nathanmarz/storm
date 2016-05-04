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

import java.io.Serializable;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public final class StormElasticSearchClient implements Serializable {

    private final EsConfig esConfig;

    public StormElasticSearchClient(EsConfig esConfig) {
        this.esConfig = esConfig;
    }

    public Client construct() {
        Settings settings = esConfig.toBasicSettings();
        TransportClient transportClient = new TransportClient(settings);
        addTransportAddresses(transportClient);
        return transportClient;
    }

    private void addTransportAddresses(TransportClient transportClient) {
        Iterable<InetSocketTransportAddress> transportAddresses = esConfig.getTransportAddresses();
        for (InetSocketTransportAddress transportAddress : transportAddresses) {
            transportClient.addTransportAddress(transportAddress);
        }
    }
}
