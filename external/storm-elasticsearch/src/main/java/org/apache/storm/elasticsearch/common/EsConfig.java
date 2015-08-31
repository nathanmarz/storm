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
package org.apache.storm.elasticsearch.common;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import static org.elasticsearch.common.base.Preconditions.checkArgument;
import static org.elasticsearch.common.base.Preconditions.checkNotNull;

/**
 * @since 0.11
 */
public class EsConfig implements Serializable {

    private final String clusterName;
    private final String[] nodes;
    private final Map<String, String> additionalConfiguration;

    /**
     * EsConfig Constructor to be used in EsIndexBolt, EsPercolateBolt and EsStateFactory
     *
     * @param clusterName Elasticsearch cluster name
     * @param nodes       Elasticsearch addresses in host:port pattern string array
     * @throws IllegalArgumentException if nodes are empty
     * @throws NullPointerException     on any of the fields being null
     */
    public EsConfig(String clusterName, String[] nodes) {
        this(clusterName, nodes, Collections.<String, String>emptyMap());
    }

    /**
     * EsConfig Constructor to be used in EsIndexBolt, EsPercolateBolt and EsStateFactory
     *
     * @param clusterName             Elasticsearch cluster name
     * @param nodes                   Elasticsearch addresses in host:port pattern string array
     * @param additionalConfiguration Additional Elasticsearch configuration
     * @throws IllegalArgumentException if nodes are empty
     * @throws NullPointerException     on any of the fields being null
     */
    public EsConfig(String clusterName, String[] nodes, Map<String, String> additionalConfiguration) {
        checkNotNull(clusterName);
        checkNotNull(nodes);
        checkNotNull(additionalConfiguration);

        checkArgument(nodes.length != 0, "Nodes cannot be empty");
        this.clusterName = clusterName;
        this.nodes = nodes;
        this.additionalConfiguration = new HashMap<>(additionalConfiguration);
    }

    TransportAddresses getTransportAddresses() {
        return new TransportAddresses(nodes);
    }

    Settings toBasicSettings() {
        return ImmutableSettings.settingsBuilder()
                                .put("cluster.name", clusterName)
                                .put(additionalConfiguration)
                                .build();
    }
}
