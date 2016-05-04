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
package org.apache.storm.scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.Config;
import org.apache.storm.networktopography.DNSToSwitchMapping;
import org.apache.storm.utils.Utils;

public class Cluster {

    /**
     * key: supervisor id, value: supervisor details
     */
    private Map<String, SupervisorDetails> supervisors;
    /**
     * key: supervisor id, value: supervisor's total and used resources
     */
    private Map<String, Double[]> supervisorsResources;

    /**
     * key: rack, value: nodes in that rack
     */
    private Map<String, List<String>> networkTopography;

    /**
     * key: topologyId, value: topology's current assignments.
     */
    private Map<String, SchedulerAssignmentImpl> assignments;
    /**
     * key topologyId, Value: scheduler's status.
     */  
    private Map<String, String> status;

    /**
     * key topologyId, Value: requested and assigned resources (e.g., on-heap/off-heap mem, cpu) for each topology.
     */
    private Map<String, Double[]> topologyResources;

    /**
     * a map from hostname to supervisor id.
     */
    private Map<String, List<String>> hostToId;

    private Map conf = null;

    private Set<String> blackListedHosts = new HashSet<String>();
    private INimbus inimbus;

    public Cluster(INimbus nimbus, Map<String, SupervisorDetails> supervisors, Map<String, SchedulerAssignmentImpl> assignments, Map storm_conf){
        this.inimbus = nimbus;
        this.supervisors = new HashMap<String, SupervisorDetails>(supervisors.size());
        this.supervisors.putAll(supervisors);
        this.assignments = new HashMap<String, SchedulerAssignmentImpl>(assignments.size());
        this.assignments.putAll(assignments);
        this.status = new HashMap<String, String>();
        this.topologyResources = new HashMap<String, Double[]>();
        this.supervisorsResources = new HashMap<String, Double[]>();
        this.hostToId = new HashMap<String, List<String>>();
        for (Map.Entry<String, SupervisorDetails> entry : supervisors.entrySet()) {
            String nodeId = entry.getKey();
            SupervisorDetails supervisor = entry.getValue();
            String host = supervisor.getHost();
            if (!this.hostToId.containsKey(host)) {
                this.hostToId.put(host, new ArrayList<String>());
            }
            this.hostToId.get(host).add(nodeId);
        }
        this.conf = storm_conf;
    }

    /**
     * Copy constructor
     */
    public Cluster(Cluster src) {
        this(src.inimbus, src.supervisors, new HashMap<String, SchedulerAssignmentImpl>(), new HashMap<String, Object>(src.conf));
        this.supervisorsResources.putAll(src.supervisorsResources);
        for (Map.Entry<String, SchedulerAssignmentImpl> entry : src.assignments.entrySet()) {
            this.assignments.put(entry.getKey(), new SchedulerAssignmentImpl(entry.getValue().getTopologyId(), entry.getValue().getExecutorToSlot()));
        }
        this.status.putAll(src.status);
        this.topologyResources.putAll(src.topologyResources);
        this.blackListedHosts.addAll(src.blackListedHosts);
    }
    
    public void setBlacklistedHosts(Set<String> hosts) {
        blackListedHosts = hosts;
    }
    
    public Set<String> getBlacklistedHosts() {
        return blackListedHosts;
    }
    
    public void blacklistHost(String host) {
        // this is so it plays well with setting blackListedHosts to an immutable list
        if(blackListedHosts==null) blackListedHosts = new HashSet<String>();
        if(!(blackListedHosts instanceof HashSet))
            blackListedHosts = new HashSet<String>(blackListedHosts);
        blackListedHosts.add(host);
    }
    
    public boolean isBlackListed(String supervisorId) {
        return blackListedHosts != null && blackListedHosts.contains(getHost(supervisorId));        
    }

    public boolean isBlacklistedHost(String host) {
        return blackListedHosts != null && blackListedHosts.contains(host);  
    }
    
    public String getHost(String supervisorId) {
        return inimbus.getHostName(supervisors, supervisorId);
    }
    
    /**
     * @return all the topologies which needs scheduling.
     */
    public List<TopologyDetails> needsSchedulingTopologies(Topologies topologies) {
        List<TopologyDetails> ret = new ArrayList<TopologyDetails>();
        for (TopologyDetails topology : topologies.getTopologies()) {
            if (needsScheduling(topology)) {
                ret.add(topology);
            }
        }

        return ret;
    }

    /**
     * Does the topology need scheduling?
     * 
     * A topology needs scheduling if one of the following conditions holds:
     * <ul>
     *   <li>Although the topology is assigned slots, but is squeezed. i.e. the topology is assigned less slots than desired.</li>
     *   <li>There are unassigned executors in this topology</li>
     * </ul>
     */
    public boolean needsScheduling(TopologyDetails topology) {
        int desiredNumWorkers = topology.getNumWorkers();
        int assignedNumWorkers = this.getAssignedNumWorkers(topology);
        return desiredNumWorkers > assignedNumWorkers || this.getUnassignedExecutors(topology).size() > 0;
    }

    /**
     * @param topology
     * @return a executor -> component-id map which needs scheduling in this topology.
     */
    public Map<ExecutorDetails, String> getNeedsSchedulingExecutorToComponents(TopologyDetails topology) {
        Collection<ExecutorDetails> allExecutors = new HashSet(topology.getExecutors());
        
        SchedulerAssignment assignment = this.assignments.get(topology.getId());
        if (assignment != null) {
            Collection<ExecutorDetails> assignedExecutors = assignment.getExecutors();
            allExecutors.removeAll(assignedExecutors);
        }

        return topology.selectExecutorToComponent(allExecutors);
    }
    
    /**
     * @param topology
     * @return a component-id -> executors map which needs scheduling in this topology.
     */
    public Map<String, List<ExecutorDetails>> getNeedsSchedulingComponentToExecutors(TopologyDetails topology) {
        Map<ExecutorDetails, String> executorToComponents = this.getNeedsSchedulingExecutorToComponents(topology);
        Map<String, List<ExecutorDetails>> componentToExecutors = new HashMap<String, List<ExecutorDetails>>();
        for (Map.Entry<ExecutorDetails, String> entry : executorToComponents.entrySet()) {
            ExecutorDetails executor = entry.getKey();
            String component = entry.getValue();
            if (!componentToExecutors.containsKey(component)) {
                componentToExecutors.put(component, new ArrayList<ExecutorDetails>());
            }
            
            componentToExecutors.get(component).add(executor);
        }
        
        return componentToExecutors;
    }


    /**
     * Get all the used ports of this supervisor.
     */
    public Set<Integer> getUsedPorts(SupervisorDetails supervisor) {
        Map<String, SchedulerAssignment> assignments = this.getAssignments();
        Set<Integer> usedPorts = new HashSet<Integer>();

        for (SchedulerAssignment assignment : assignments.values()) {
            for (WorkerSlot slot : assignment.getExecutorToSlot().values()) {
                if (slot.getNodeId().equals(supervisor.getId())) {
                    usedPorts.add(slot.getPort());
                }
            }
        }

        return usedPorts;
    }

    /**
     * Return the available ports of this supervisor.
     */
    public Set<Integer> getAvailablePorts(SupervisorDetails supervisor) {
        Set<Integer> usedPorts = this.getUsedPorts(supervisor);

        Set<Integer> ret = new HashSet();
        ret.addAll(getAssignablePorts(supervisor));
        ret.removeAll(usedPorts);

        return ret;
    }
    
    public Set<Integer> getAssignablePorts(SupervisorDetails supervisor) {
        if(isBlackListed(supervisor.id)) return new HashSet();
        return supervisor.allPorts;
    }

    /**
     * Return all the available slots on this supervisor.
     */
    public List<WorkerSlot> getAvailableSlots(SupervisorDetails supervisor) {
        Set<Integer> ports = this.getAvailablePorts(supervisor);
        List<WorkerSlot> slots = new ArrayList<WorkerSlot>(ports.size());

        for (Integer port : ports) {
            slots.add(new WorkerSlot(supervisor.getId(), port));
        }

        return slots;
    }
    
    public List<WorkerSlot> getAssignableSlots(SupervisorDetails supervisor) {
        Set<Integer> ports = this.getAssignablePorts(supervisor);
        List<WorkerSlot> slots = new ArrayList<WorkerSlot>(ports.size());

        for (Integer port : ports) {
            slots.add(new WorkerSlot(supervisor.getId(), port));
        }

        return slots;        
    }
    
    /**
     * get the unassigned executors of the topology.
     */
    public Collection<ExecutorDetails> getUnassignedExecutors(TopologyDetails topology) {
        if (topology == null) {
            return new ArrayList<ExecutorDetails>(0);
        }

        Collection<ExecutorDetails> ret = new HashSet(topology.getExecutors());
        
        SchedulerAssignment assignment = this.getAssignmentById(topology.getId());
        if (assignment != null) {
            Set<ExecutorDetails> assignedExecutors = assignment.getExecutors();
            ret.removeAll(assignedExecutors);
        }
        
        return ret;
    }

    /**
     * @param topology
     * @return the number of workers assigned to this topology.
     */
    public int getAssignedNumWorkers(TopologyDetails topology) {
        SchedulerAssignment assignment = topology != null ? this.getAssignmentById(topology.getId()) : null;
        if (assignment == null) {
            return 0;
        }

        Set<WorkerSlot> slots = new HashSet<WorkerSlot>();
        slots.addAll(assignment.getExecutorToSlot().values());
        return slots.size();
    }

    /**
     * Assign the slot to the executors for this topology.
     * 
     * @throws RuntimeException if the specified slot is already occupied.
     */
    public void assign(WorkerSlot slot, String topologyId, Collection<ExecutorDetails> executors) {
        if (this.isSlotOccupied(slot)) {
            throw new RuntimeException("slot: [" + slot.getNodeId() + ", " + slot.getPort() + "] is already occupied.");
        }
        
        SchedulerAssignmentImpl assignment = (SchedulerAssignmentImpl)this.getAssignmentById(topologyId);
        if (assignment == null) {
            assignment = new SchedulerAssignmentImpl(topologyId, new HashMap<ExecutorDetails, WorkerSlot>());
            this.assignments.put(topologyId, assignment);
        } else {
            for (ExecutorDetails executor : executors) {
                 if (assignment.isExecutorAssigned(executor)) {
                     throw new RuntimeException("the executor is already assigned, you should unassign it before assign it to another slot.");
                 }
            }
        }

        assignment.assign(slot, executors);
    }

    /**
     * @return all the available worker slots in the cluster.
     */
    public List<WorkerSlot> getAvailableSlots() {
        List<WorkerSlot> slots = new ArrayList<WorkerSlot>();
        for (SupervisorDetails supervisor : this.supervisors.values()) {
            slots.addAll(this.getAvailableSlots(supervisor));
        }

        return slots;
    }
    
    public List<WorkerSlot> getAssignableSlots() {
        List<WorkerSlot> slots = new ArrayList<WorkerSlot>();
        for (SupervisorDetails supervisor : this.supervisors.values()) {
            slots.addAll(this.getAssignableSlots(supervisor));
        }

        return slots;
    }

    /**
     * Free the specified slot.
     * 
     * @param slot
     */
    public void freeSlot(WorkerSlot slot) {
        // remove the slot from the existing assignments
        for (SchedulerAssignmentImpl assignment : this.assignments.values()) {
            if (assignment.isSlotOccupied(slot)) {
                assignment.unassignBySlot(slot);
            }
        }
    }
    
    /**
     * free the slots.
     * 
     * @param slots
     */
    public void freeSlots(Collection<WorkerSlot> slots) {
        if(slots!=null) {
            for (WorkerSlot slot : slots) {
                this.freeSlot(slot);
            }
        }
    }

    /**
     * @param slot the slot be to checked.
     * @return true if the specified slot is occupied.
     */
    public boolean isSlotOccupied(WorkerSlot slot) {
        for (SchedulerAssignment assignment : this.assignments.values()) {
            if (assignment.isSlotOccupied(slot)) {
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * get the current assignment for the topology.
     */
    public SchedulerAssignment getAssignmentById(String topologyId) {
        if (this.assignments.containsKey(topologyId)) {
            return this.assignments.get(topologyId);
        }

        return null;
    }

    /**
     * get slots used by a topology
     */
    public Collection<WorkerSlot> getUsedSlotsByTopologyId(String topologyId) {
        if (!this.assignments.containsKey(topologyId)) {
            return null;
        }
        return this.assignments.get(topologyId).getSlots();
    }

    /**
     * Get a specific supervisor with the <code>nodeId</code>
     */
    public SupervisorDetails getSupervisorById(String nodeId) {
        if (this.supervisors.containsKey(nodeId)) {
            return this.supervisors.get(nodeId);
        }

        return null;
    }
    
    public Collection<WorkerSlot> getUsedSlots() {
        Set<WorkerSlot> ret = new HashSet();
        for(SchedulerAssignmentImpl s: assignments.values()) {
            ret.addAll(s.getExecutorToSlot().values());
        }
        return ret;
    }

    /**
     * Get all the supervisors on the specified <code>host</code>.
     * 
     * @param host hostname of the supervisor
     * @return the <code>SupervisorDetails</code> object.
     */
    public List<SupervisorDetails> getSupervisorsByHost(String host) {
        List<String> nodeIds = this.hostToId.get(host);
        List<SupervisorDetails> ret = new ArrayList<SupervisorDetails>();

        if (nodeIds != null) {
            for (String nodeId : nodeIds) {
                ret.add(this.getSupervisorById(nodeId));
            }
        }

        return ret;
    }

    /**
     * Get all the assignments.
     */
    public Map<String, SchedulerAssignment> getAssignments() {
        Map<String, SchedulerAssignment> ret = new HashMap<String, SchedulerAssignment>(this.assignments.size());
        
        for (String topologyId : this.assignments.keySet()) {
            ret.put(topologyId, this.assignments.get(topologyId));
        }

        return ret;
    }

    /**
     * set assignments for cluster
     */
    public void setAssignments(Map<String, SchedulerAssignment> newAssignments) {
        this.assignments = new HashMap<String, SchedulerAssignmentImpl>(newAssignments.size());
        for (Map.Entry<String, SchedulerAssignment> entry : newAssignments.entrySet()) {
            this.assignments.put(entry.getKey(), new SchedulerAssignmentImpl(entry.getValue().getTopologyId(), entry.getValue().getExecutorToSlot()));
        }
    }

    /**
     * Get all the supervisors.
     */
    public Map<String, SupervisorDetails> getSupervisors() {
        return this.supervisors;
    }

    /**
     * Get the total amount of CPU resources in cluster
     */
    public double getClusterTotalCPUResource() {
        double sum = 0.0;
        for (SupervisorDetails sup : this.supervisors.values()) {
            sum += sup.getTotalCPU();
        }
        return sum;
    }

    /**
     * Get the total amount of memory resources in cluster
     */
    public double getClusterTotalMemoryResource() {
        double sum = 0.0;
        for (SupervisorDetails sup : this.supervisors.values()) {
            sum += sup.getTotalMemory();
        }
        return sum;
    }

    /*
    * Note: Make sure the proper conf was passed into the Cluster constructor before calling this function
    * It tries to load the proper network topography detection plugin specified in the config.
    */
    public Map<String, List<String>> getNetworkTopography() {
        if (networkTopography == null) {
            networkTopography = new HashMap<String, List<String>>();
            ArrayList<String> supervisorHostNames = new ArrayList<String>();
            for (SupervisorDetails s : supervisors.values()) {
                supervisorHostNames.add(s.getHost());
            }

            String clazz = (String) conf.get(Config.STORM_NETWORK_TOPOGRAPHY_PLUGIN);
            DNSToSwitchMapping topographyMapper = (DNSToSwitchMapping) Utils.newInstance(clazz);

            Map<String, String> resolvedSuperVisors = topographyMapper.resolve(supervisorHostNames);
            for (Map.Entry<String, String> entry : resolvedSuperVisors.entrySet()) {
                String hostName = entry.getKey();
                String rack = entry.getValue();
                List<String> nodesForRack = networkTopography.get(rack);
                if (nodesForRack == null) {
                    nodesForRack = new ArrayList<String>();
                    networkTopography.put(rack, nodesForRack);
                }
                nodesForRack.add(hostName);
            }
        }
        return networkTopography;
    }

    private String getStringFromStringList(Object o) {
        StringBuilder sb = new StringBuilder();
        for (String s : (List<String>) o) {
            sb.append(s);
            sb.append(" ");
        }
        return sb.toString();
    }

    /*
    * Get heap memory usage for a worker's main process and logwriter process
    * */
    private Double getAssignedMemoryForSlot(Map topConf) {
        Double totalWorkerMemory = 0.0;
        final Integer TOPOLOGY_WORKER_DEFAULT_MEMORY_ALLOCATION = 768;

        String topologyWorkerGcChildopts = null;
        if (topConf.get(Config.TOPOLOGY_WORKER_GC_CHILDOPTS) instanceof List) {
            topologyWorkerGcChildopts = getStringFromStringList(topConf.get(Config.TOPOLOGY_WORKER_GC_CHILDOPTS));
        } else {
            topologyWorkerGcChildopts = Utils.getString(topConf.get(Config.TOPOLOGY_WORKER_GC_CHILDOPTS), null);
        }

        String workerGcChildopts = null;
        if (topConf.get(Config.WORKER_GC_CHILDOPTS) instanceof List) {
            workerGcChildopts = getStringFromStringList(topConf.get(Config.WORKER_GC_CHILDOPTS));
        } else {
            workerGcChildopts = Utils.getString(topConf.get(Config.WORKER_GC_CHILDOPTS), null);
        }

        Double memGcChildopts = null;
        memGcChildopts = Utils.parseJvmHeapMemByChildOpts(topologyWorkerGcChildopts, null);
        if (memGcChildopts == null) {
            memGcChildopts = Utils.parseJvmHeapMemByChildOpts(workerGcChildopts, null);
        }

        String topologyWorkerChildopts = null;
        if (topConf.get(Config.TOPOLOGY_WORKER_CHILDOPTS) instanceof List) {
            topologyWorkerChildopts = getStringFromStringList(topConf.get(Config.TOPOLOGY_WORKER_CHILDOPTS));
        } else {
            topologyWorkerChildopts = Utils.getString(topConf.get(Config.TOPOLOGY_WORKER_CHILDOPTS), null);
        }
        Double memTopologyWorkerChildopts = Utils.parseJvmHeapMemByChildOpts(topologyWorkerChildopts, null);

        String workerChildopts = null;
        if (topConf.get(Config.WORKER_CHILDOPTS) instanceof List) {
            workerChildopts = getStringFromStringList(topConf.get(Config.WORKER_CHILDOPTS));
        } else {
            workerChildopts = Utils.getString(topConf.get(Config.WORKER_CHILDOPTS), null);
        }
        Double memWorkerChildopts = Utils.parseJvmHeapMemByChildOpts(workerChildopts, null);

        if (memGcChildopts != null) {
            totalWorkerMemory += memGcChildopts;
        } else if (memTopologyWorkerChildopts != null) {
            totalWorkerMemory += memTopologyWorkerChildopts;
        } else if (memWorkerChildopts != null) {
            totalWorkerMemory += memWorkerChildopts;
        } else {
            totalWorkerMemory += Utils.getInt(topConf.get(Config.WORKER_HEAP_MEMORY_MB), TOPOLOGY_WORKER_DEFAULT_MEMORY_ALLOCATION);
        }

        String topoWorkerLwChildopts = null;
        if (topConf.get(Config.TOPOLOGY_WORKER_LOGWRITER_CHILDOPTS) instanceof List) {
            topoWorkerLwChildopts = getStringFromStringList(topConf.get(Config.TOPOLOGY_WORKER_LOGWRITER_CHILDOPTS));
        } else {
            topoWorkerLwChildopts = Utils.getString(topConf.get(Config.TOPOLOGY_WORKER_LOGWRITER_CHILDOPTS), null);
        }
        if (topoWorkerLwChildopts != null) {
            totalWorkerMemory += Utils.parseJvmHeapMemByChildOpts(topoWorkerLwChildopts, 0.0);
        }
        return totalWorkerMemory;
    }

    /*
    * Update memory usage for each topology and each supervisor node after every round of scheduling
    * */
    public void updateAssignedMemoryForTopologyAndSupervisor(Topologies topologies) {
        Map<String, Double> supervisorToAssignedMem = new HashMap<String, Double>();

        for (Map.Entry<String, SchedulerAssignment> entry : this.getAssignments().entrySet()) {
            String topId = entry.getValue().getTopologyId();
            Map topConf = topologies.getById(topId).getConf();
            Double assignedMemForTopology = 0.0;
            Double assignedMemPerSlot = getAssignedMemoryForSlot(topConf);
            for (WorkerSlot ws: entry.getValue().getSlots()) {
                assignedMemForTopology += assignedMemPerSlot;
                String nodeId = ws.getNodeId();
                if (supervisorToAssignedMem.containsKey(nodeId)) {
                    supervisorToAssignedMem.put(nodeId, supervisorToAssignedMem.get(nodeId) + assignedMemPerSlot);
                } else {
                    supervisorToAssignedMem.put(nodeId, assignedMemPerSlot);
                }
            }
            if (this.getTopologyResourcesMap().containsKey(topId)) {
                Double[] topo_resources = getTopologyResourcesMap().get(topId);
                topo_resources[3] = assignedMemForTopology;
            } else {
                Double[] topo_resources = {0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
                topo_resources[3] = assignedMemForTopology;
                this.setTopologyResources(topId, topo_resources);
            }
        }

        for (Map.Entry<String, Double> entry : supervisorToAssignedMem.entrySet()) {
            String nodeId = entry.getKey();
            if (this.supervisorsResources.containsKey(nodeId)) {
                Double[] supervisor_resources = supervisorsResources.get(nodeId);
                supervisor_resources[2] = entry.getValue();
            } else {
                Double[] supervisor_resources = {0.0, 0.0, 0.0, 0.0};
                supervisor_resources[2] = entry.getValue();
                this.supervisorsResources.put(nodeId, supervisor_resources);
            }
        }
    }

    /**
     * set scheduler status for a topology
     */
    public void setStatus(String topologyId, String status) {
        this.status.put(topologyId, status);
    }

    /**
     * Get all topology scheduler statuses
     */
    public Map<String, String> getStatusMap() {
        return this.status;
    }

    /**
     * set scheduler status map
     */
    public void setStatusMap(Map<String, String> statusMap) {
        this.status.clear();
        this.status.putAll(statusMap);
    }

    /**
     * Set the amount of resources used used by a topology. Used for displaying resource information on the UI
     * @param topologyId
     * @param resources describes the resources requested and assigned to topology in the following format in an array:
     *  {requestedMemOnHeap, requestedMemOffHeap, requestedCpu, assignedMemOnHeap, assignedMemOffHeap, assignedCpu}
     */
    public void setTopologyResources(String topologyId, Double[] resources) {
        this.topologyResources.put(topologyId, resources);
    }

    /**
     * Set the amount of resources used used by a topology. Used for displaying resource information on the UI
     * @param topologyResources a map that contains multiple topologies and the resources the topology requested and assigned.
     * Key: topology id Value: an array that describes the resources the topology requested and assigned in the following format:
     *  {requestedMemOnHeap, requestedMemOffHeap, requestedCpu, assignedMemOnHeap, assignedMemOffHeap, assignedCpu}
     */
    public void setTopologyResourcesMap(Map<String, Double[]> topologyResources) {
        this.topologyResources.putAll(topologyResources);
    }

    /**
     * Get the amount of resources used by topologies.  Used for displaying resource information on the UI
     * @return  a map that contains multiple topologies and the resources the topology requested and assigned.
     * Key: topology id Value: an array that describes the resources the topology requested and assigned in the following format:
     *  {requestedMemOnHeap, requestedMemOffHeap, requestedCpu, assignedMemOnHeap, assignedMemOffHeap, assignedCpu}
     */
    public Map<String, Double[]> getTopologyResourcesMap() {
        return this.topologyResources;
    }

    /**
     * Sets the amount of used and free resources on a supervisor. Used for displaying resource information on the UI
     * @param supervisorResources a map where the key is the supervisor id and the value is a map that represents
     * resource usage for a supervisor in the following format: {totalMem, totalCpu, usedMem, usedCpu}
     */
    public void setSupervisorsResourcesMap(Map<String, Double[]> supervisorResources) {
        this.supervisorsResources.putAll(supervisorResources);
    }

    /**
     * Get the amount of used and free resources on a supervisor.  Used for displaying resource information on the UI
     * @return  a map where the key is the supervisor id and the value is a map that represents
     * resource usage for a supervisor in the following format: {totalMem, totalCpu, usedMem, usedCpu}
     */
    public Map<String, Double[]> getSupervisorsResourcesMap() {
        return this.supervisorsResources;
    }
}
