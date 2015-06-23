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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.apache.storm.elasticsearch.common.EsConfig;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class AbstractEsBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractEsBolt.class);
    protected static Client client;
    protected OutputCollector collector;
    private EsConfig esConfig;

    public AbstractEsBolt(EsConfig esConfig) {
        this.esConfig = esConfig;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        try {
            this.collector = outputCollector;
            synchronized (AbstractEsBolt.class) {
                if (client == null) {
                    Settings settings =
                            ImmutableSettings.settingsBuilder().put("cluster.name", esConfig.getClusterName())
                                    .put("client.transport.sniff", "true").build();
                    List<InetSocketTransportAddress> transportAddressList = new ArrayList<InetSocketTransportAddress>();
                    for (String node : esConfig.getNodes()) {
                        String[] hostAndPort = node.split(":");
                        if(hostAndPort.length != 2){
                            throw new IllegalArgumentException("incorrect Elasticsearch node format, should follow {host}:{port} pattern");
                        }
                        transportAddressList.add(new InetSocketTransportAddress(hostAndPort[0], Integer.parseInt(hostAndPort[1])));
                    }
                    client = new TransportClient(settings)
                            .addTransportAddresses(transportAddressList.toArray(new InetSocketTransportAddress[transportAddressList.size()]));
                }
            }
        } catch (Exception e) {
            LOG.warn("unable to initialize EsBolt ", e);
        }
    }

    @Override
    public void execute(Tuple tuple) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}
