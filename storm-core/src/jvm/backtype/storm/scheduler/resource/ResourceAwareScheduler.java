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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.scheduler.resource.strategies.ResourceAwareStrategy;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

public class ResourceAwareScheduler implements IScheduler {
    private static final Logger LOG = LoggerFactory
            .getLogger(ResourceAwareScheduler.class);
    @SuppressWarnings("rawtypes")
    private Map _conf;

    @Override
    public void prepare(Map conf) {
        _conf = conf;
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        LOG.debug("\n\n\nRerunning ResourceAwareScheduler...");

        ResourceAwareStrategy RAStrategy = new ResourceAwareStrategy(cluster, topologies);
        LOG.debug(printScheduling(cluster, topologies));

        for (TopologyDetails td : topologies.getTopologies()) {
            String topId = td.getId();
            Map<WorkerSlot, Collection<ExecutorDetails>> schedulerAssignmentMap;
            if (cluster.getUnassignedExecutors(td).size() > 0) {
                LOG.debug("/********Scheduling topology {} ************/", topId);

                schedulerAssignmentMap = RAStrategy.schedule(td);

                double requestedMemOnHeap = td.getTotalRequestedMemOnHeap();
                double requestedMemOffHeap = td.getTotalRequestedMemOffHeap();
                double requestedCpu = td.getTotalRequestedCpu();
                double assignedMemOnHeap = 0.0;
                double assignedMemOffHeap = 0.0;
                double assignedCpu = 0.0;

                if (schedulerAssignmentMap != null) {
                    try {
                        Set<String> nodesUsed = new HashSet<String>();
                        int assignedWorkers = schedulerAssignmentMap.keySet().size();
                        for (Map.Entry<WorkerSlot, Collection<ExecutorDetails>> workerToTasksEntry : schedulerAssignmentMap.entrySet()) {
                            WorkerSlot targetSlot = workerToTasksEntry.getKey();
                            Collection<ExecutorDetails> execsNeedScheduling = workerToTasksEntry.getValue();
                            RAS_Node targetNode = RAStrategy.idToNode(targetSlot.getNodeId());
                            targetNode.assign(targetSlot, td, execsNeedScheduling, cluster);
                            LOG.debug("ASSIGNMENT    TOPOLOGY: {}  TASKS: {} To Node: {} on Slot: {}",
                                    td.getName(), execsNeedScheduling, targetNode.getHostname(), targetSlot.getPort());
                            if (!nodesUsed.contains(targetNode.getId())) {
                                nodesUsed.add(targetNode.getId());
                            }
                            assignedMemOnHeap += targetSlot.getAllocatedMemOnHeap();
                            assignedMemOffHeap += targetSlot.getAllocatedMemOffHeap();
                            assignedCpu += targetSlot.getAllocatedCpu();
                        }
                        LOG.debug("Topology: {} assigned to {} nodes on {} workers", td.getId(), nodesUsed.size(), assignedWorkers);
                        cluster.setStatus(td.getId(), "Fully Scheduled");
                    } catch (IllegalStateException ex) {
                        LOG.error(ex.toString());
                        LOG.error("Unsuccessful in scheduling", td.getId());
                        cluster.setStatus(td.getId(), "Unsuccessful in scheduling");
                    }
                } else {
                    LOG.error("Unsuccessful in scheduling", td.getId());
                    cluster.setStatus(td.getId(), "Unsuccessful in scheduling");
                }
                Double[] resources = {requestedMemOnHeap, requestedMemOffHeap, requestedCpu,
                        assignedMemOnHeap, assignedMemOffHeap, assignedCpu};
                LOG.debug("setResources for {}: requested on-heap mem, off-heap mem, cpu: {} {} {} " +
                        "assigned on-heap mem, off-heap mem, cpu: {} {} {}",
                        td.getId(), requestedMemOnHeap, requestedMemOffHeap, requestedCpu,
                        assignedMemOnHeap, assignedMemOffHeap, assignedCpu);
                cluster.setResources(td.getId(), resources);
            } else {
                cluster.setStatus(td.getId(), "Fully Scheduled");
            }
        }
        updateSupervisorsResources(cluster, topologies);
    }

    private void updateSupervisorsResources(Cluster cluster, Topologies topologies) {
        Map<String, Double[]> supervisors_resources = new HashMap<String, Double[]>();
        Map<String, RAS_Node> nodes = RAS_Node.getAllNodesFrom(cluster, topologies);
        for (Map.Entry<String, RAS_Node> entry : nodes.entrySet()) {
            RAS_Node node = entry.getValue();
            Double totalMem = node.getTotalMemoryResources();
            Double totalCpu = node.getTotalCpuResources();
            Double usedMem = totalMem - node.getAvailableMemoryResources();
            Double usedCpu = totalCpu - node.getAvailableCpuResources();
            Double[] resources = {totalMem, totalCpu, usedMem, usedCpu};
            supervisors_resources.put(entry.getKey(), resources);
        }
        cluster.setSupervisorsResourcesMap(supervisors_resources);
    }

    private Map<String, Double> getUserConf() {
        Map<String, Double> ret = new HashMap<String, Double>();
        ret.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB,
                (Double) _conf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB));
        ret.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB,
                (Double) _conf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB));
        return ret;
    }

    /**
     * print scheduling for debug purposes
     * @param cluster
     * @param topologies
     */
    public String printScheduling(Cluster cluster, Topologies topologies) {
        StringBuilder str = new StringBuilder();
        Map<String, Map<String, Map<WorkerSlot, Collection<ExecutorDetails>>>> schedulingMap = new HashMap<String, Map<String, Map<WorkerSlot, Collection<ExecutorDetails>>>>();
        for (TopologyDetails topo : topologies.getTopologies()) {
            if (cluster.getAssignmentById(topo.getId()) != null) {
                for (Map.Entry<ExecutorDetails, WorkerSlot> entry : cluster.getAssignmentById(topo.getId()).getExecutorToSlot().entrySet()) {
                    WorkerSlot slot = entry.getValue();
                    String nodeId = slot.getNodeId();
                    ExecutorDetails exec = entry.getKey();
                    if (schedulingMap.containsKey(nodeId) == false) {
                        schedulingMap.put(nodeId, new HashMap<String, Map<WorkerSlot, Collection<ExecutorDetails>>>());
                    }
                    if (schedulingMap.get(nodeId).containsKey(topo.getId()) == false) {
                        schedulingMap.get(nodeId).put(topo.getId(), new HashMap<WorkerSlot, Collection<ExecutorDetails>>());
                    }
                    if (schedulingMap.get(nodeId).get(topo.getId()).containsKey(slot) == false) {
                        schedulingMap.get(nodeId).get(topo.getId()).put(slot, new LinkedList<ExecutorDetails>());
                    }
                    schedulingMap.get(nodeId).get(topo.getId()).get(slot).add(exec);
                }
            }
        }

        for (Map.Entry<String, Map<String, Map<WorkerSlot, Collection<ExecutorDetails>>>> entry : schedulingMap.entrySet()) {
            if (cluster.getSupervisorById(entry.getKey()) != null) {
                str.append("/** Node: " + cluster.getSupervisorById(entry.getKey()).getHost() + "-" + entry.getKey() + " **/\n");
            } else {
                str.append("/** Node: Unknown may be dead -" + entry.getKey() + " **/\n");
            }
            for (Map.Entry<String, Map<WorkerSlot, Collection<ExecutorDetails>>> topo_sched : schedulingMap.get(entry.getKey()).entrySet()) {
                str.append("\t-->Topology: " + topo_sched.getKey() + "\n");
                for (Map.Entry<WorkerSlot, Collection<ExecutorDetails>> ws : topo_sched.getValue().entrySet()) {
                    str.append("\t\t->Slot [" + ws.getKey().getPort() + "] -> " + ws.getValue() + "\n");
                }
            }
        }
        return str.toString();
    }
}
