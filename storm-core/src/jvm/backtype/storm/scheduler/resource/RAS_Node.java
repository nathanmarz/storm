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
    private Map<String, Set<WorkerSlot>> topIdToUsedSlots = new HashMap<String, Set<WorkerSlot>>();
    private Set<WorkerSlot> freeSlots = new HashSet<WorkerSlot>();
    private final String nodeId;
    private String hostname;
    private boolean isAlive;
    private SupervisorDetails sup;
    private Double availMemory;
    private Double availCPU;
    private Cluster cluster;
    private Topologies topologies;

    public RAS_Node(String nodeId, Set<Integer> allPorts, boolean isAlive,
                    SupervisorDetails sup, Cluster cluster, Topologies topologies) {
        this.nodeId = nodeId;
        this.isAlive = isAlive;
        if (this.isAlive && allPorts != null) {
            for (int port : allPorts) {
                this.freeSlots.add(new WorkerSlot(this.nodeId, port));
            }
            this.sup = sup;
            this.hostname = sup.getHost();
            this.availMemory = getTotalMemoryResources();
            this.availCPU = getTotalCpuResources();
        }
        this.cluster = cluster;
        this.topologies = topologies;
    }

    public String getId() {
        return this.nodeId;
    }

    public String getHostname() {
        return this.hostname;
    }

    public Collection<WorkerSlot> getFreeSlots() {
        return this.freeSlots;
    }

    public Collection<WorkerSlot> getUsedSlots() {
        Collection<WorkerSlot> ret = new LinkedList<WorkerSlot>();
        for (Collection<WorkerSlot> workers : this.topIdToUsedSlots.values()) {
            ret.addAll(workers);
        }
        return ret;
    }

    public boolean isAlive() {
        return this.isAlive;
    }

    /**
     * @return a collection of the topology ids currently running on this node
     */
    public Collection<String> getRunningTopologies() {
        return this.topIdToUsedSlots.keySet();
    }

    public boolean isTotallyFree() {
        return this.topIdToUsedSlots.isEmpty();
    }

    public int totalSlotsFree() {
        return this.freeSlots.size();
    }

    public int totalSlotsUsed() {
        int total = 0;
        for (Set<WorkerSlot> slots : this.topIdToUsedSlots.values()) {
            total += slots.size();
        }
        return total;
    }

    public int totalSlots() {
        return totalSlotsFree() + totalSlotsUsed();
    }

    public int totalSlotsUsed(String topId) {
        int total = 0;
        Set<WorkerSlot> slots = this.topIdToUsedSlots.get(topId);
        if (slots != null) {
            total = slots.size();
        }
        return total;
    }

    private void validateSlot(WorkerSlot ws) {
        if (!this.nodeId.equals(ws.getNodeId())) {
            throw new IllegalArgumentException(
                    "Trying to add a slot to the wrong node " + ws +
                            " is not a part of " + this.nodeId);
        }
    }

    void addOrphanedSlot(WorkerSlot ws) {
        if (this.isAlive) {
            throw new IllegalArgumentException("Orphaned Slots " +
                    "only are allowed on dead nodes.");
        }
        validateSlot(ws);
        if (this.freeSlots.contains(ws)) {
            return;
        }
        for (Set<WorkerSlot> used : this.topIdToUsedSlots.values()) {
            if (used.contains(ws)) {
                return;
            }
        }
        this.freeSlots.add(ws);
    }

    boolean assignInternal(WorkerSlot ws, String topId, boolean dontThrow) {
        validateSlot(ws);
        if (!this.freeSlots.remove(ws)) {
            if (dontThrow) {
                return true;
            }
            throw new IllegalStateException("Assigning a slot that was not free " + ws);
        }
        Set<WorkerSlot> usedSlots = this.topIdToUsedSlots.get(topId);
        if (usedSlots == null) {
            usedSlots = new HashSet<WorkerSlot>();
            this.topIdToUsedSlots.put(topId, usedSlots);
        }
        usedSlots.add(ws);
        return false;
    }

    /**
     * Free all slots on this node.  This will update the Cluster too.
     */
    public void freeAllSlots() {
        if (!this.isAlive) {
            LOG.warn("Freeing all slots on a dead node {} ", this.nodeId);
        }
        for (Entry<String, Set<WorkerSlot>> entry : this.topIdToUsedSlots.entrySet()) {
            this.cluster.freeSlots(entry.getValue());
            this.availCPU = this.getTotalCpuResources();
            this.availMemory = this.getAvailableMemoryResources();
            if (this.isAlive) {
                this.freeSlots.addAll(entry.getValue());
            }
        }
        this.topIdToUsedSlots = new HashMap<String, Set<WorkerSlot>>();
    }

    /**
     * Frees a single slot in this node
     * @param ws the slot to free
     */
    public void free(WorkerSlot ws) {
        LOG.info("freeing ws {} on node {}", ws, this.hostname);
        if (this.freeSlots.contains(ws)) return;
        for (Entry<String, Set<WorkerSlot>> entry : this.topIdToUsedSlots.entrySet()) {
            Set<WorkerSlot> slots = entry.getValue();
            double memUsed = this.getMemoryUsedByWorker(ws);
            double cpuUsed = this.getCpuUsedByWorker(ws);
            if (slots.remove(ws)) {
                this.cluster.freeSlot(ws);
                if (this.isAlive) {
                    this.freeSlots.add(ws);
                }
                this.freeMemory(memUsed);
                this.freeCPU(cpuUsed);
                return;
            }
        }
        throw new IllegalArgumentException("Tried to free a slot that was not" +
                " part of this node " + this.nodeId);
    }

    /**
     * Frees all the slots for a topology.
     * @param topId the topology to free slots for
     */
    public void freeTopology(String topId) {
        Set<WorkerSlot> slots = this.topIdToUsedSlots.get(topId);
        if (slots == null || slots.isEmpty()) {
            return;
        }
        for (WorkerSlot ws : slots) {
            this.cluster.freeSlot(ws);
            this.freeMemory(this.getMemoryUsedByWorker(ws));
            this.freeCPU(this.getCpuUsedByWorker(ws));
            if (this.isAlive) {
                this.freeSlots.add(ws);
            }
        }
        this.topIdToUsedSlots.remove(topId);
    }

    private void freeMemory(double amount) {
        LOG.debug("freeing {} memory on node {}...avail mem: {}", amount, this.getHostname(), this.availMemory);
        if((this.availMemory + amount) > this.getTotalCpuResources()) {
            LOG.warn("Freeing more memory than there exists!");
            return;
        }
        this.availMemory += amount;
    }

    private void freeCPU(double amount) {
        LOG.debug("freeing {} CPU on node...avail CPU: {}", amount, this.getHostname(), this.availCPU);
        if ((this.availCPU + amount) > this.getAvailableCpuResources()) {
            LOG.warn("Freeing more CPU than there exists!");
            return;
        }
        this.availCPU += amount;
    }

    public double getMemoryUsedByWorker(WorkerSlot ws) {
        TopologyDetails topo = this.findTopologyUsingWorker(ws);
        if (topo == null) {
            return 0.0;
        }
        Collection<ExecutorDetails> execs = this.getExecutors(ws, this.cluster);
        double totalMemoryUsed = 0.0;
        for (ExecutorDetails exec : execs) {
            totalMemoryUsed += topo.getTotalMemReqTask(exec);
        }
        return totalMemoryUsed;
    }

    public double getCpuUsedByWorker(WorkerSlot ws) {
        TopologyDetails topo = this.findTopologyUsingWorker(ws);
        if (topo == null) {
            return 0.0;
        }
        Collection<ExecutorDetails> execs = this.getExecutors(ws, this.cluster);
        double totalCpuUsed = 0.0;
        for (ExecutorDetails exec : execs) {
            totalCpuUsed += topo.getTotalCpuReqTask(exec);
        }
        return totalCpuUsed;
    }

    public TopologyDetails findTopologyUsingWorker(WorkerSlot ws) {
        for (Entry<String, Set<WorkerSlot>> entry : this.topIdToUsedSlots.entrySet()) {
            String topoId = entry.getKey();
            Set<WorkerSlot> workers = entry.getValue();
            for (WorkerSlot worker : workers) {
                if (worker.getNodeId().equals(ws.getNodeId()) && worker.getPort() == ws.getPort()) {
                    return this.topologies.getById(topoId);
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
        if (!this.isAlive) {
            throw new IllegalStateException("Trying to adding to a dead node " + this.nodeId);
        }
        if (this.freeSlots.isEmpty()) {
            throw new IllegalStateException("Trying to assign to a full node " + this.nodeId);
        }
        if (executors.size() == 0) {
            LOG.warn("Trying to assign nothing from " + td.getId() + " to " + this.nodeId + " (Ignored)");
        }

        if (target == null) {
            target = this.freeSlots.iterator().next();
        }
        if (!this.freeSlots.contains(target)) {
            throw new IllegalStateException("Trying to assign already used slot" + target.getPort() + "on node " + this.nodeId);
        } else {
            allocateResourceToSlot(td, executors, target);
            this.cluster.assign(target, td.getId(), executors);
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
        this.assign(null, td, executors);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof RAS_Node) {
            return this.nodeId.equals(((RAS_Node) other).nodeId);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return this.nodeId.hashCode();
    }

    @Override
    public String toString() {
        return "{Node: " + ((this.sup == null) ? "null (possibly down)" : this.sup.getHost())
                + ", AvailMem: " + ((this.availMemory == null) ? "N/A" : this.availMemory.toString())
                + ", this.availCPU: " + ((this.availCPU == null) ? "N/A" : this.availCPU.toString()) + "}";
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
        this.availMemory = amount;
    }

    /**
     * Gets the available memory resources for this node
     * @return the available memory for this node
     */
    public Double getAvailableMemoryResources() {
        if (this.availMemory == null) {
            return 0.0;
        }
        return this.availMemory;
    }

    /**
     * Gets the total memory resources for this node
     * @return the total memory for this node
     */
    public Double getTotalMemoryResources() {
        if (this.sup != null && this.sup.getTotalMemory() != null) {
            return this.sup.getTotalMemory();
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
        if (amount > this.availMemory) {
            LOG.error("Attempting to consume more memory than available! Needed: {}, we only have: {}", amount, this.availMemory);
            return null;
        }
        this.availMemory = this.availMemory - amount;
        return this.availMemory;
    }

    /**
     * Gets the available cpu resources for this node
     * @return the available cpu for this node
     */
    public Double getAvailableCpuResources() {
        if (this.availCPU == null) {
            return 0.0;
        }
        return this.availCPU;
    }

    /**
     * Gets the total cpu resources for this node
     * @return the total cpu for this node
     */
    public Double getTotalCpuResources() {
        if (this.sup != null && this.sup.getTotalCPU() != null) {
            return this.sup.getTotalCPU();
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
        if (amount > this.availCPU) {
            LOG.error("Attempting to consume more CPU than available! Needed: {}, we only have: {}", amount, this.availCPU);
            return null;
        }
        this.availCPU = this.availCPU - amount;
        return this.availCPU;
    }

    /**
     * Consumes a certain amount of resources for a executor in a topology.
     * @param exec is the executor that is consuming resources on this node
     * @param topo the topology the executor is a part
     */
    public void consumeResourcesforTask(ExecutorDetails exec, TopologyDetails topo) {
        Double taskMemReq = topo.getTotalMemReqTask(exec);
        Double taskCpuReq = topo.getTotalCpuReqTask(exec);
        this.consumeCPU(taskCpuReq);
        this.consumeMemory(taskMemReq);
    }

    public Map<String, Set<WorkerSlot>> getTopoIdTousedSlots() {
        return this.topIdToUsedSlots;
    }
}
