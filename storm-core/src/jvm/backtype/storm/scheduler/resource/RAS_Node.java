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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.ArrayList;

import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.WorkerSlot;

/**
 * Represents a single node in the cluster.
 */
public class RAS_Node {
    private static final Logger LOG = LoggerFactory.getLogger(RAS_Node.class);
    private Map<String, Set<WorkerSlot>> _topIdToUsedSlots = new HashMap<String, Set<WorkerSlot>>();
    private Set<WorkerSlot> _freeSlots = new HashSet<WorkerSlot>();
    private final String _nodeId;
    private String _hostname;
    private boolean _isAlive;
    private SupervisorDetails _sup;
    private Double _availMemory;
    private Double _availCPU;
    private Cluster _cluster;
    private Topologies _topologies;

    public RAS_Node(String nodeId, Set<Integer> allPorts, boolean isAlive,
                    SupervisorDetails sup, Cluster cluster, Topologies topologies) {
        _nodeId = nodeId;
        _isAlive = isAlive;
        if (_isAlive && allPorts != null) {
            for (int port : allPorts) {
                _freeSlots.add(new WorkerSlot(_nodeId, port));
            }
            _sup = sup;
            _hostname = sup.getHost();
            _availMemory = getTotalMemoryResources();
            _availCPU = getTotalCpuResources();
        }
        _cluster = cluster;
        _topologies = topologies;
    }

    public String getId() {
        return _nodeId;
    }

    public String getHostname() {
        return _hostname;
    }

    public Collection<WorkerSlot> getFreeSlots() {
        return _freeSlots;
    }

    public Collection<WorkerSlot> getUsedSlots() {
        Collection<WorkerSlot> ret = new LinkedList<WorkerSlot>();
        for (Collection<WorkerSlot> workers : _topIdToUsedSlots.values()) {
            ret.addAll(workers);
        }
        return ret;
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
        return _topIdToUsedSlots.isEmpty();
    }

    public int totalSlotsFree() {
        return _freeSlots.size();
    }

    public int totalSlotsUsed() {
        int total = 0;
        for (Set<WorkerSlot> slots : _topIdToUsedSlots.values()) {
            total += slots.size();
        }
        return total;
    }

    public int totalSlots() {
        return totalSlotsFree() + totalSlotsUsed();
    }

    public int totalSlotsUsed(String topId) {
        int total = 0;
        Set<WorkerSlot> slots = _topIdToUsedSlots.get(topId);
        if (slots != null) {
            total = slots.size();
        }
        return total;
    }

    private void validateSlot(WorkerSlot ws) {
        if (!_nodeId.equals(ws.getNodeId())) {
            throw new IllegalArgumentException(
                    "Trying to add a slot to the wrong node " + ws +
                            " is not a part of " + _nodeId);
        }
    }

    void addOrphanedSlot(WorkerSlot ws) {
        if (_isAlive) {
            throw new IllegalArgumentException("Orphaned Slots " +
                    "only are allowed on dead nodes.");
        }
        validateSlot(ws);
        if (_freeSlots.contains(ws)) {
            return;
        }
        for (Set<WorkerSlot> used : _topIdToUsedSlots.values()) {
            if (used.contains(ws)) {
                return;
            }
        }
        _freeSlots.add(ws);
    }

    boolean assignInternal(WorkerSlot ws, String topId, boolean dontThrow) {
        validateSlot(ws);
        if (!_freeSlots.remove(ws)) {
            if (dontThrow) {
                return true;
            }
            throw new IllegalStateException("Assigning a slot that was not free " + ws);
        }
        Set<WorkerSlot> usedSlots = _topIdToUsedSlots.get(topId);
        if (usedSlots == null) {
            usedSlots = new HashSet<WorkerSlot>();
            _topIdToUsedSlots.put(topId, usedSlots);
        }
        usedSlots.add(ws);
        return false;
    }

    /**
     * Free all slots on this node.  This will update the Cluster too.
     */
    public void freeAllSlots() {
        if (!_isAlive) {
            LOG.warn("Freeing all slots on a dead node {} ", _nodeId);
        }
        for (Entry<String, Set<WorkerSlot>> entry : _topIdToUsedSlots.entrySet()) {
            _cluster.freeSlots(entry.getValue());
            _availCPU = getTotalCpuResources();
            _availMemory = getAvailableMemoryResources();
            if (_isAlive) {
                _freeSlots.addAll(entry.getValue());
            }
        }
        _topIdToUsedSlots = new HashMap<String, Set<WorkerSlot>>();
    }

    /**
     * Frees a single slot in this node
     * @param ws the slot to free
     */
    public void free(WorkerSlot ws) {
        LOG.info("freeing WorkerSlot {} on node {}", ws, _hostname);
        if (_freeSlots.contains(ws)) return;
        for (Entry<String, Set<WorkerSlot>> entry : _topIdToUsedSlots.entrySet()) {
            Set<WorkerSlot> slots = entry.getValue();
            double memUsed = getMemoryUsedByWorker(ws);
            double cpuUsed = getCpuUsedByWorker(ws);
            if (slots.remove(ws)) {
                _cluster.freeSlot(ws);
                if (_isAlive) {
                    _freeSlots.add(ws);
                }
                freeMemory(memUsed);
                freeCPU(cpuUsed);
                return;
            }
        }
        throw new IllegalArgumentException("Tried to free a slot that was not" +
                " part of this node " + _nodeId);
    }

    /**
     * Frees all the slots for a topology.
     * @param topId the topology to free slots for
     */
    public void freeTopology(String topId) {
        Set<WorkerSlot> slots = _topIdToUsedSlots.get(topId);
        if (slots == null || slots.isEmpty()) {
            return;
        }
        for (WorkerSlot ws : slots) {
            _cluster.freeSlot(ws);
            freeMemory(getMemoryUsedByWorker(ws));
            freeCPU(getCpuUsedByWorker(ws));
            if (_isAlive) {
                _freeSlots.add(ws);
            }
        }
        _topIdToUsedSlots.remove(topId);
    }

    private void freeMemory(double amount) {
        LOG.debug("freeing {} memory on node {}...avail mem: {}", amount, getHostname(), _availMemory);
        if((_availMemory + amount) > getTotalCpuResources()) {
            LOG.warn("Freeing more memory than there exists!");
            return;
        }
        _availMemory += amount;
    }

    private void freeCPU(double amount) {
        LOG.debug("freeing {} CPU on node...avail CPU: {}", amount, getHostname(), _availCPU);
        if ((_availCPU + amount) > getAvailableCpuResources()) {
            LOG.warn("Freeing more CPU than there exists!");
            return;
        }
        _availCPU += amount;
    }

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

    public TopologyDetails findTopologyUsingWorker(WorkerSlot ws) {
        for (Entry<String, Set<WorkerSlot>> entry : _topIdToUsedSlots.entrySet()) {
            String topoId = entry.getKey();
            Set<WorkerSlot> workers = entry.getValue();
            for (WorkerSlot worker : workers) {
                if (worker.getNodeId().equals(ws.getNodeId()) && worker.getPort() == ws.getPort()) {
                    return _topologies.getById(topoId);
                }
            }
        }
        return null;
    }

    /**
     * Allocate Mem and CPU resources to the assigned slot for the topology's executors.
     * @param td the TopologyDetails that the slot is assigned to.
     * @param executors the executors to run in that slot.
     * @param slot the slot to allocate resource to
     */
    public void allocateResourceToSlot (TopologyDetails td, Collection<ExecutorDetails> executors, WorkerSlot slot) {
        double onHeapMem = 0.0;
        double offHeapMem = 0.0;
        double cpu = 0.0;
        for (ExecutorDetails exec : executors) {
            Double onHeapMemForExec = td.getOnHeapMemoryRequirement(exec);
            if (onHeapMemForExec != null) {
                onHeapMem += onHeapMemForExec;
            }
            Double offHeapMemForExec = td.getOffHeapMemoryRequirement(exec);
            if (offHeapMemForExec != null) {
                offHeapMem += offHeapMemForExec;
            }
            Double cpuForExec = td.getTotalCpuReqTask(exec);
            if (cpuForExec != null) {
                cpu += cpuForExec;
            }
        }
        slot.allocateResource(onHeapMem, offHeapMem, cpu);
    }

    public void assign(WorkerSlot target, TopologyDetails td, Collection<ExecutorDetails> executors) {
        if (!_isAlive) {
            throw new IllegalStateException("Trying to adding to a dead node " + _nodeId);
        }
        if (_freeSlots.isEmpty()) {
            throw new IllegalStateException("Trying to assign to a full node " + _nodeId);
        }
        if (executors.size() == 0) {
            LOG.warn("Trying to assign nothing from " + td.getId() + " to " + _nodeId + " (Ignored)");
        }

        if (target == null) {
            target = _freeSlots.iterator().next();
        }
        if (!_freeSlots.contains(target)) {
            throw new IllegalStateException("Trying to assign already used slot" + target.getPort() + "on node " + _nodeId);
        } else {
            allocateResourceToSlot(td, executors, target);
            _cluster.assign(target, td.getId(), executors);
            assignInternal(target, td.getId(), false);
        }
    }

    /**
     * Assign a free slot on the node to the following topology and executors.
     * This will update the cluster too.
     * @param td the TopologyDetails to assign a free slot to.
     * @param executors the executors to run in that slot.
     */
    public void assign(TopologyDetails td, Collection<ExecutorDetails> executors) {
        assign(null, td, executors);
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
            return null;
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
            return null;
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

    public Map<String, Set<WorkerSlot>> getTopoIdTousedSlots() {
        return _topIdToUsedSlots;
    }
}
