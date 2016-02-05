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
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.WorkerSlot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class RAS_Nodes {

    private Map<String, RAS_Node> nodeMap;

    private static final Logger LOG = LoggerFactory.getLogger(RAS_Nodes.class);

    public RAS_Nodes(Cluster cluster, Topologies topologies) {
        this.nodeMap = getAllNodesFrom(cluster, topologies);
    }

    public static Map<String, RAS_Node> getAllNodesFrom(Cluster cluster, Topologies topologies) {

        //A map of node ids to node objects
        Map<String, RAS_Node> nodeIdToNode = new HashMap<String, RAS_Node>();
        //A map of assignments organized by node with the following format:
        //{nodeId -> {topologyId -> {workerId -> {execs}}}}
        Map<String, Map<String, Map<String, Collection<ExecutorDetails>>>> assignmentRelationshipMap
                = new HashMap<String, Map<String, Map<String, Collection<ExecutorDetails>>>>();

        Map<String, Map<String, WorkerSlot>> workerIdToWorker = new HashMap<String, Map<String, WorkerSlot>>();
        for (SchedulerAssignment assignment : cluster.getAssignments().values()) {
            String topId = assignment.getTopologyId();

            for (Map.Entry<WorkerSlot, Collection<ExecutorDetails>> entry : assignment.getSlotToExecutors().entrySet()) {
                WorkerSlot slot = entry.getKey();
                String nodeId = slot.getNodeId();
                Collection<ExecutorDetails> execs = entry.getValue();
                if (!assignmentRelationshipMap.containsKey(nodeId)) {
                    assignmentRelationshipMap.put(nodeId, new HashMap<String, Map<String, Collection<ExecutorDetails>>>());
                    workerIdToWorker.put(nodeId, new HashMap<String, WorkerSlot>());
                }
                workerIdToWorker.get(nodeId).put(slot.getId(), slot);
                if (!assignmentRelationshipMap.get(nodeId).containsKey(topId)) {
                    assignmentRelationshipMap.get(nodeId).put(topId, new HashMap<String, Collection<ExecutorDetails>>());
                }
                if (!assignmentRelationshipMap.get(nodeId).get(topId).containsKey(slot.getId())) {
                    assignmentRelationshipMap.get(nodeId).get(topId).put(slot.getId(), new LinkedList<ExecutorDetails>());
                }
                assignmentRelationshipMap.get(nodeId).get(topId).get(slot.getId()).addAll(execs);
            }
        }

        for (SupervisorDetails sup : cluster.getSupervisors().values()) {
            //Initialize a worker slot for every port even if there is no assignment to it
            for (int port : sup.getAllPorts()) {
                WorkerSlot worker = new WorkerSlot(sup.getId(), port);
                if (!workerIdToWorker.containsKey(sup.getId())) {
                    workerIdToWorker.put(sup.getId(), new HashMap<String, WorkerSlot>());
                }
                if (!workerIdToWorker.get(sup.getId()).containsKey(worker.getId())) {
                    workerIdToWorker.get(sup.getId()).put(worker.getId(), worker);
                }
            }
            nodeIdToNode.put(sup.getId(), new RAS_Node(sup.getId(), sup, cluster, topologies, workerIdToWorker.get(sup.getId()), assignmentRelationshipMap.get(sup.getId())));
        }

        //Add in supervisors that might have crashed but workers are still alive
        for(Map.Entry<String, Map<String, Map<String, Collection<ExecutorDetails>>>> entry : assignmentRelationshipMap.entrySet()) {
            String nodeId = entry.getKey();
            Map<String, Map<String, Collection<ExecutorDetails>>> assignments = entry.getValue();
            if (!nodeIdToNode.containsKey(nodeId)) {
                LOG.info("Found an assigned slot(s) on a dead supervisor {} with assignments {}",
                        nodeId, assignments);
                nodeIdToNode.put(nodeId, new RAS_Node(nodeId, null, cluster, topologies, workerIdToWorker.get(nodeId), assignments));
            }
        }
        return nodeIdToNode;
    }

    /**
     * get node object from nodeId
     */
    public RAS_Node getNodeById(String nodeId) {
        return this.nodeMap.get(nodeId);
    }

    /**
     *
     * @param workerSlots
     */
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
