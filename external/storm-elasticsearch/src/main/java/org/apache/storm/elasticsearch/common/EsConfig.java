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

public class EsConfig implements Serializable{
    private String clusterName;
    private String[] nodes;

    public EsConfig() {
    }

    /**
     * EsConfig Constructor to be used in EsIndexBolt, EsPercolateBolt and EsStateFactory
     * @param clusterName Elasticsearch cluster name
     * @param nodes Elasticsearch addresses in host:port pattern string array
     */
    public EsConfig(String clusterName, String[] nodes) {
        this.clusterName = clusterName;
        this.nodes = nodes;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String[] getNodes() {
        return nodes;
    }

    public void setNodes(String[] nodes) {
        this.nodes = nodes;
    }
}
