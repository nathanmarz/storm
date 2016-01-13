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

package org.apache.storm.scheduler.resource;

import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A class to specify which data and API to expose to a scheduling strategy
 */
public class ClusterStateData {

    private final Cluster cluster;

    public final Topologies topologies;

    // Information regarding all nodes in the cluster
    public Map<String, NodeDetails> nodes = new HashMap<String, NodeDetails>();

    public static final class NodeDetails {

        private final RAS_Node node;

        public NodeDetails(RAS_Node node) {
            this.node = node;
        }

        public String getId() {
            return this.node.getId();
        }

        public String getHostname() {
            return this.node.getHostname();
        }

        public Collection<WorkerSlot> getFreeSlots() {
            return this.node.getFreeSlots();
        }

        public void consumeResourcesforTask(ExecutorDetails exec, TopologyDetails topo) {
            this.node.consumeResourcesforTask(exec, topo);
        }

        public Double getAvailableMemoryResources() {
            return this.node.getAvailableMemoryResources();
        }

        public Double getAvailableCpuResources() {
            return this.node.getAvailableCpuResources();
        }

        public Double getTotalMemoryResources() {
            return this.node.getTotalMemoryResources();
        }

        public Double getTotalCpuResources() {
            return this.node.getTotalCpuResources();
        }
    }

    public ClusterStateData(Cluster cluster, Topologies topologies) {
        this.cluster = cluster;
        this.topologies = topologies;
        Map<String, RAS_Node> nodes = RAS_Nodes.getAllNodesFrom(cluster, topologies);
        for (Map.Entry<String, RAS_Node> entry : nodes.entrySet()) {
            this.nodes.put(entry.getKey(), new NodeDetails(entry.getValue()));
        }
    }

    public Collection<ExecutorDetails> getUnassignedExecutors(String topoId) {
        return this.cluster.getUnassignedExecutors(this.topologies.getById(topoId));
    }

    public Map<String, List<String>> getNetworkTopography() {
        return this.cluster.getNetworkTopography();
    }
}