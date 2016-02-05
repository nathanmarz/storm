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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.ArrayList;

import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.WorkerSlot;

/**
 * Represents a single node in the cluster.
 */
public class RAS_Node {
    private static final Logger LOG = LoggerFactory.getLogger(RAS_Node.class);

    //A map consisting of all workers on the node.
    //The key of the map is the worker id and the value is the corresponding workerslot object
    Map<String, WorkerSlot> _slots = new HashMap<String, WorkerSlot> ();

    // A map describing which topologies are using which slots on this node.  The format of the map is the following:
    // {TopologyId -> {WorkerId -> {Executors}}}
    private Map<String, Map<String, Collection<ExecutorDetails>>> _topIdToUsedSlots = new HashMap<String, Map<String, Collection<ExecutorDetails>>>();

    private final String _nodeId;
    private String _hostname;
    private boolean _isAlive;
    private SupervisorDetails _sup;
    private Double _availMemory = 0.0;
    private Double _availCPU = 0.0;
    private final Cluster _cluster;
    private final Topologies _topologies;

    public RAS_Node(String nodeId, SupervisorDetails sup, Cluster cluster, Topologies topologies, Map<String, WorkerSlot> workerIdToWorker, Map<String, Map<String, Collection<ExecutorDetails>>> assignmentMap) {
        //Node ID and supervisor ID are the same.
        _nodeId = nodeId;
        if (sup == null) {
            _isAlive = false;
        } else {
            _isAlive = !cluster.isBlackListed(_nodeId);
        }

        _cluster = cluster;
        _topologies = topologies;

        // initialize slots for this node
        if (workerIdToWorker != null) {
            _slots = workerIdToWorker;
        }

        //initialize assignment map
        if (assignmentMap != null) {
            _topIdToUsedSlots = assignmentMap;
        }

        //check if node is alive
        if (_isAlive && sup != null) {
            _hostname = sup.getHost();
            _sup = sup;
            _availMemory = getTotalMemoryResources();
            _availCPU = getTotalCpuResources();

            LOG.debug("Found a {} Node {} {}",
                    _isAlive ? "living" : "dead", _nodeId, sup.getAllPorts());
            LOG.debug("resources_mem: {}, resources_CPU: {}", sup.getTotalMemory(), sup.getTotalCPU());
            //intialize resource usages on node
            intializeResources();
        }
    }

    /**
     * initializes resource usages on node
     */
    private void intializeResources() {
        for (Entry<String, Map<String, Collection<ExecutorDetails>>> entry : _topIdToUsedSlots.entrySet()) {
            String topoId = entry.getKey();
            Map<String, Collection<ExecutorDetails>> assignment = entry.getValue();
            Map<ExecutorDetails, Double> topoMemoryResourceList = _topologies.getById(topoId).getTotalMemoryResourceList();
            for (Collection<ExecutorDetails> execs : assignment.values()) {
                for (ExecutorDetails exec : execs) {
                    if (!_isAlive) {
                        continue;
                        // We do not free the assigned slots (the orphaned slots) on the inactive supervisors
                        // The inactive node will be treated as a 0-resource node and not available for other unassigned workers
                    }
                    if (topoMemoryResourceList.containsKey(exec)) {
                        consumeResourcesforTask(exec, _topologies.getById(topoId));
                    } else {
                        throw new IllegalStateException("Executor " + exec + "not found!");
                    }
                }
            }
        }
    }

    public String getId() {
        return _nodeId;
    }

    public String getHostname() {
        return _hostname;
    }

    private Collection<WorkerSlot> workerIdsToWorkers(Collection<String> workerIds) {
        Collection<WorkerSlot> ret = new LinkedList<WorkerSlot>();
        for (String workerId : workerIds) {
            ret.add(_slots.get(workerId));
        }
        return ret;
    }

    public Collection<String> getFreeSlotsId() {
        if (!_isAlive) {
            return new HashSet<String>();
        }
        Collection<String> usedSlotsId = getUsedSlotsId();
        Set<String> ret = new HashSet<>();
        ret.addAll(_slots.keySet());
        ret.removeAll(usedSlotsId);
        return ret;
    }

    public Collection<WorkerSlot> getFreeSlots() {
        return workerIdsToWorkers(getFreeSlotsId());
    }

    public Collection<String> getUsedSlotsId() {
        Collection<String> ret = new LinkedList<String>();
        for (Map<String, Collection<ExecutorDetails>> entry : _topIdToUsedSlots.values()) {
            ret.addAll(entry.keySet());
        }
        return ret;
    }

    public Collection<WorkerSlot> getUsedSlots() {
        return workerIdsToWorkers(getUsedSlotsId());
    }

    public Collection<WorkerSlot> getUsedSlots(String topId) {
        return workerIdsToWorkers(_topIdToUsedSlots.get(topId).keySet());
    }

    public boolean isAlive() {
        return _isAlive;
    }

    /**
     * @return a collection of the topology ids currently running on this node
     */
    public Collection<String> getRunningTopologies() {
        return _topIdToUsedSlots.keySet();
    }

    public boolean isTotallyFree() {
        return getUsedSlots().isEmpty();
    }

    public int totalSlotsFree() {
        return getFreeSlots().size();
    }

    public int totalSlotsUsed() {
        return getUsedSlots().size();
    }

    public int totalSlots() {
        return _slots.size();
    }

    public int totalSlotsUsed(String topId) {
        return getUsedSlots(topId).size();
    }

    /**
     * Free all slots on this node.  This will update the Cluster too.
     */
    public void freeAllSlots() {
        if (!_isAlive) {
            LOG.warn("Freeing all slots on a dead node {} ", _nodeId);
        }
        _cluster.freeSlots(_slots.values());
        _availCPU = getTotalCpuResources();
        _availMemory = getAvailableMemoryResources();
        //clearing assignments
        _topIdToUsedSlots.clear();
    }

    /**
     * Frees a single slot in this node
     * @param ws the slot to free
     */
    public void free(WorkerSlot ws) {
        LOG.info("freeing WorkerSlot {} on node {}", ws, _hostname);
        if (!_slots.containsKey(ws.getId())) {
            throw new IllegalArgumentException("Tried to free a slot " + ws + " that was not" +
                    " part of this node " + _nodeId);
        }

        TopologyDetails topo = findTopologyUsingWorker(ws);
        if (topo == null) {
            throw new IllegalArgumentException("Tried to free a slot " + ws + " that was already free!");
        }

        double memUsed = getMemoryUsedByWorker(ws);
        double cpuUsed = getCpuUsedByWorker(ws);
        freeMemory(memUsed);
        freeCPU(cpuUsed);

        //free slot
        _cluster.freeSlot(ws);
        //cleanup internal assignments
        _topIdToUsedSlots.get(topo.getId()).remove(ws.getId());
    }

    private void freeMemory(double amount) {
        LOG.debug("freeing {} memory on node {}...avail mem: {}", amount, getHostname(), _availMemory);
        if((_availMemory + amount) > getTotalMemoryResources()) {
            LOG.warn("Freeing more memory than there exists! Memory trying to free: {} Total memory on Node: {}", (_availMemory + amount), getTotalMemoryResources());
            return;
        }
        _availMemory += amount;
    }

    private void freeCPU(double amount) {
        LOG.debug("freeing {} CPU on node...avail CPU: {}", amount, getHostname(), _availCPU);
        if ((_availCPU + amount) > getTotalCpuResources()) {
            LOG.warn("Freeing more CPU than there exists! CPU trying to free: {} Total CPU on Node: {}", (_availMemory + amount), getTotalCpuResources());
            return;
        }
        _availCPU += amount;
    }

    /**
     * get the amount of memory used by a worker
     */
    public double getMemoryUsedByWorker(WorkerSlot ws) {
        TopologyDetails topo = findTopologyUsingWorker(ws);
        if (topo == null) {
            return 0.0;
        }
        Collection<ExecutorDetails> execs = getExecutors(ws, _cluster);
        double totalMemoryUsed = 0.0;
        for (ExecutorDetails exec : execs) {
            totalMemoryUsed += topo.getTotalMemReqTask(exec);
        }
        return totalMemoryUsed;
    }

    /**
     * get the amount of cpu used by a worker
     */
    public double getCpuUsedByWorker(WorkerSlot ws) {
        TopologyDetails topo = findTopologyUsingWorker(ws);
        if (topo == null) {
            return 0.0;
        }
        Collection<ExecutorDetails> execs = getExecutors(ws, _cluster);
        double totalCpuUsed = 0.0;
        for (ExecutorDetails exec : execs) {
            totalCpuUsed += topo.getTotalCpuReqTask(exec);
        }
        return totalCpuUsed;
    }

    /**
     * Find a which topology is running on a worker slot
     * @return the topology using the worker slot.  If worker slot is free then return null
     */
    public TopologyDetails findTopologyUsingWorker(WorkerSlot ws) {
        for (Entry<String, Map<String, Collection<ExecutorDetails>>> entry : _topIdToUsedSlots.entrySet()) {
            String topoId = entry.getKey();
            Set<String> workerIds = entry.getValue().keySet();
            for (String workerId : workerIds) {
                if(ws.getId().equals(workerId)) {
                    return _topologies.getById(topoId);
                }
            }
        }
        return null;
    }

    /**
     * Assigns a worker to a node
     * @param target the worker slot to assign the executors
     * @param td the topology the executors are from
     * @param executors executors to assign to the specified worker slot
     */
    public void assign(WorkerSlot target, TopologyDetails td, Collection<ExecutorDetails> executors) {
        if (!_isAlive) {
            throw new IllegalStateException("Trying to adding to a dead node " + _nodeId);
        }
        Collection<WorkerSlot> freeSlots = getFreeSlots();
        if (freeSlots.isEmpty()) {
            throw new IllegalStateException("Trying to assign to a full node " + _nodeId);
        }
        if (executors.size() == 0) {
            LOG.warn("Trying to assign nothing from " + td.getId() + " to " + _nodeId + " (Ignored)");
        }
        if (target == null) {
            target = getFreeSlots().iterator().next();
        }
        if (!freeSlots.contains(target)) {
            throw new IllegalStateException("Trying to assign already used slot" + target.getPort() + "on node " + _nodeId);
        }
        LOG.info("target slot: {}", target);

        _cluster.assign(target, td.getId(), executors);

        //assigning internally
        if (!_topIdToUsedSlots.containsKey(td.getId())) {
            _topIdToUsedSlots.put(td.getId(), new HashMap<String, Collection<ExecutorDetails>>());
        }

        if (!_topIdToUsedSlots.get(td.getId()).containsKey(target.getId())) {
            _topIdToUsedSlots.get(td.getId()).put(target.getId(), new LinkedList<ExecutorDetails>());
        }
        _topIdToUsedSlots.get(td.getId()).get(target.getId()).addAll(executors);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof RAS_Node) {
            return _nodeId.equals(((RAS_Node) other)._nodeId);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return _nodeId.hashCode();
    }

    @Override
    public String toString() {
        return "{Node: " + ((_sup == null) ? "null (possibly down)" : _sup.getHost())
                + ", AvailMem: " + ((_availMemory == null) ? "N/A" : _availMemory.toString())
                + ", AvailCPU: " + ((_availCPU == null) ? "N/A" : _availCPU.toString()) + "}";
    }

    public static int countSlotsUsed(String topId, Collection<RAS_Node> nodes) {
        int total = 0;
        for (RAS_Node n : nodes) {
            total += n.totalSlotsUsed(topId);
        }
        return total;
    }

    public static int countSlotsUsed(Collection<RAS_Node> nodes) {
        int total = 0;
        for (RAS_Node n : nodes) {
            total += n.totalSlotsUsed();
        }
        return total;
    }

    public static int countFreeSlotsAlive(Collection<RAS_Node> nodes) {
        int total = 0;
        for (RAS_Node n : nodes) {
            if (n.isAlive()) {
                total += n.totalSlotsFree();
            }
        }
        return total;
    }

    public static int countTotalSlotsAlive(Collection<RAS_Node> nodes) {
        int total = 0;
        for (RAS_Node n : nodes) {
            if (n.isAlive()) {
                total += n.totalSlots();
            }
        }
        return total;
    }

    public static Collection<ExecutorDetails> getExecutors(WorkerSlot ws, Cluster cluster) {
        Collection<ExecutorDetails> retList = new ArrayList<ExecutorDetails>();
        for (Entry<String, SchedulerAssignment> entry : cluster.getAssignments()
                .entrySet()) {
            Map<ExecutorDetails, WorkerSlot> executorToSlot = entry.getValue()
                    .getExecutorToSlot();
            for (Map.Entry<ExecutorDetails, WorkerSlot> execToSlot : executorToSlot
                    .entrySet()) {
                WorkerSlot slot = execToSlot.getValue();
                if (ws.getPort() == slot.getPort()
                        && ws.getNodeId().equals(slot.getNodeId())) {
                    ExecutorDetails exec = execToSlot.getKey();
                    retList.add(exec);
                }
            }
        }
        return retList;
    }

    /**
     * Sets the Available Memory for a node
     * @param amount the amount to set as available memory
     */
    public void setAvailableMemory(Double amount) {
        _availMemory = amount;
    }

    /**
     * Gets the available memory resources for this node
     * @return the available memory for this node
     */
    public Double getAvailableMemoryResources() {
        if (_availMemory == null) {
            return 0.0;
        }
        return _availMemory;
    }

    /**
     * Gets the total memory resources for this node
     * @return the total memory for this node
     */
    public Double getTotalMemoryResources() {
        if (_sup != null && _sup.getTotalMemory() != null) {
            return _sup.getTotalMemory();
        } else {
            return 0.0;
        }
    }

    /**
     * Consumes a certain amount of memory for this node
     * @param amount is the amount memory to consume from this node
     * @return the current available memory for this node after consumption
     */
    public Double consumeMemory(Double amount) {
        if (amount > _availMemory) {
            LOG.error("Attempting to consume more memory than available! Needed: {}, we only have: {}", amount, _availMemory);
            throw new IllegalStateException("Attempting to consume more memory than available");
        }
        _availMemory = _availMemory - amount;
        return _availMemory;
    }

    /**
     * Gets the available cpu resources for this node
     * @return the available cpu for this node
     */
    public Double getAvailableCpuResources() {
        if (_availCPU == null) {
            return 0.0;
        }
        return _availCPU;
    }

    /**
     * Gets the total cpu resources for this node
     * @return the total cpu for this node
     */
    public Double getTotalCpuResources() {
        if (_sup != null && _sup.getTotalCPU() != null) {
            return _sup.getTotalCPU();
        } else {
            return 0.0;
        }
    }

    /**
     * Consumes a certain amount of cpu for this node
     * @param amount is the amount cpu to consume from this node
     * @return the current available cpu for this node after consumption
     */
    public Double consumeCPU(Double amount) {
        if (amount > _availCPU) {
            LOG.error("Attempting to consume more CPU than available! Needed: {}, we only have: {}", amount, _availCPU);
            throw new IllegalStateException("Attempting to consume more memory than available");
        }
        _availCPU = _availCPU - amount;
        return _availCPU;
    }

    /**
     * Consumes a certain amount of resources for a executor in a topology.
     * @param exec is the executor that is consuming resources on this node
     * @param topo the topology the executor is a part
     */
    public void consumeResourcesforTask(ExecutorDetails exec, TopologyDetails topo) {
        Double taskMemReq = topo.getTotalMemReqTask(exec);
        Double taskCpuReq = topo.getTotalCpuReqTask(exec);
        consumeCPU(taskCpuReq);
        consumeMemory(taskMemReq);
    }
}
