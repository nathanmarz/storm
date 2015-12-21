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

package backtype.storm.scheduler.resource;

import backtype.storm.Config;
import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.WorkerSlot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class RAS_Nodes {

    private Map<String, RAS_Node> nodeMap;

    private static final Logger LOG = LoggerFactory.getLogger(RAS_Nodes.class);

    public RAS_Nodes(Cluster cluster, Topologies topologies) {
        this.nodeMap = getAllNodesFrom(cluster, topologies);
    }

    public static Map<String, RAS_Node> getAllNodesFrom(Cluster cluster, Topologies topologies) {
        Map<String, RAS_Node> nodeIdToNode = new HashMap<String, RAS_Node>();
        for (SupervisorDetails sup : cluster.getSupervisors().values()) {
            //Node ID and supervisor ID are the same.
            String id = sup.getId();
            boolean isAlive = !cluster.isBlackListed(id);
            LOG.debug("Found a {} Node {} {}",
                    isAlive ? "living" : "dead", id, sup.getAllPorts());
            LOG.debug("resources_mem: {}, resources_CPU: {}", sup.getTotalMemory(), sup.getTotalCPU());
            nodeIdToNode.put(sup.getId(), new RAS_Node(id, sup.getAllPorts(), isAlive, sup, cluster, topologies));
        }
        for (SchedulerAssignment assignment : cluster.getAssignments().values()) {
            String topId = assignment.getTopologyId();
            for (WorkerSlot workerSlot : assignment.getSlots()) {
                String id = workerSlot.getNodeId();
                RAS_Node node = nodeIdToNode.get(id);
                if (node == null) {
                    LOG.info("Found an assigned slot on a dead supervisor {} with executors {}",
                            workerSlot, RAS_Node.getExecutors(workerSlot, cluster));
                    node = new RAS_Node(id, null, false, null, cluster, topologies);
                    nodeIdToNode.put(id, node);
                }
                if (!node.isAlive()) {
                    //The supervisor on the node is down so add an orphaned slot to hold the unsupervised worker
                    node.addOrphanedSlot(workerSlot);
                }
                if (node.assignInternal(workerSlot, topId, true)) {
                    LOG.warn("Bad scheduling state, {} assigned multiple workers, unassigning everything...", workerSlot);
                    node.free(workerSlot);
                }
            }
        }
        updateAvailableResources(cluster, topologies, nodeIdToNode);
        return nodeIdToNode;
    }

    /**
     * updates the available resources for every node in a cluster
     * by recalculating memory requirements.
     *
     * @param cluster      the cluster used in this calculation
     * @param topologies   container of all topologies
     * @param nodeIdToNode a map between node id and node
     */
    private static void updateAvailableResources(Cluster cluster,
                                                 Topologies topologies,
                                                 Map<String, RAS_Node> nodeIdToNode) {
        //recompute memory
        if (cluster.getAssignments().size() > 0) {
            for (Map.Entry<String, SchedulerAssignment> entry : cluster.getAssignments()
                    .entrySet()) {
                Map<ExecutorDetails, WorkerSlot> executorToSlot = entry.getValue()
                        .getExecutorToSlot();
                Map<ExecutorDetails, Double> topoMemoryResourceList = topologies.getById(entry.getKey()).getTotalMemoryResourceList();

                if (topoMemoryResourceList == null || topoMemoryResourceList.size() == 0) {
                    continue;
                }
                for (Map.Entry<ExecutorDetails, WorkerSlot> execToSlot : executorToSlot
                        .entrySet()) {
                    WorkerSlot slot = execToSlot.getValue();
                    ExecutorDetails exec = execToSlot.getKey();
                    RAS_Node node = nodeIdToNode.get(slot.getNodeId());
                    if (!node.isAlive()) {
                        continue;
                        // We do not free the assigned slots (the orphaned slots) on the inactive supervisors
                        // The inactive node will be treated as a 0-resource node and not available for other unassigned workers
                    }
                    if (topoMemoryResourceList.containsKey(exec)) {
                        node.consumeResourcesforTask(exec, topologies.getById(entry.getKey()));
                    } else {
                        LOG.warn("Resource Req not found...Scheduling Task {} with memory requirement as on heap - {} " +
                                        "and off heap - {} and CPU requirement as {}",
                                exec,
                                Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB,
                                Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT);
                        topologies.getById(entry.getKey()).addDefaultResforExec(exec);
                        node.consumeResourcesforTask(exec, topologies.getById(entry.getKey()));
                    }
                }
            }
        } else {
            for (RAS_Node n : nodeIdToNode.values()) {
                n.setAvailableMemory(n.getAvailableMemoryResources());
            }
        }
    }

    public RAS_Node getNodeById(String nodeId) {
        return this.nodeMap.get(nodeId);
    }

    public void freeSlots(Collection<WorkerSlot> workerSlots) {
        for (RAS_Node node : nodeMap.values()) {
            for (WorkerSlot ws : node.getUsedSlots()) {
                if (workerSlots.contains(ws)) {
                    LOG.debug("freeing ws {} on node {}", ws, node);
                    node.free(ws);
                }
            }
        }
    }

    public Collection<RAS_Node> getNodes() {
        return this.nodeMap.values();
    }

    @Override
    public String toString() {
        String ret = "";
        for (RAS_Node node : this.nodeMap.values()) {
            ret += node.toString() + "\n";
        }
        return ret;
    }
}
