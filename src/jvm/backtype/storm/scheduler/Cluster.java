package backtype.storm.scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Cluster {

    /**
     * key: supervisor id, value: supervisor details
     */
    private Map<String, SupervisorDetails>   supervisors;
    /**
     * key: topologyId, value: topology's current assignments.
     */
    private Map<String, SchedulerAssignmentImpl> assignments;

    /**
     * a map from hostname to supervisor id.
     */
    private Map<String, List<String>>        hostToId;
    
    private Set<String> blackListedHosts = new HashSet<String>();
    private INimbus inimbus;

    public Cluster(INimbus nimbus, Map<String, SupervisorDetails> supervisors, Map<String, SchedulerAssignmentImpl> assignments){
        this.inimbus = nimbus;
        this.supervisors = new HashMap<String, SupervisorDetails>(supervisors.size());
        this.supervisors.putAll(supervisors);
        this.assignments = new HashMap<String, SchedulerAssignmentImpl>(assignments.size());
        this.assignments.putAll(assignments);
        this.hostToId = new HashMap<String, List<String>>();
        for (String nodeId : supervisors.keySet()) {
            SupervisorDetails supervisor = supervisors.get(nodeId);
            String host = supervisor.getHost();
            if (!this.hostToId.containsKey(host)) {
                this.hostToId.put(host, new ArrayList<String>());
            }
            this.hostToId.get(host).add(nodeId);
        }
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
    
    public String getHost(String supervisorId) {
        return inimbus.getHostName(supervisors, supervisorId);
    }
    
    /**
     * Gets all the topologies which needs scheduling.
     * 
     * @param topologies
     * @return
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

        if (desiredNumWorkers > assignedNumWorkers) {
            return true;
        }

        return this.getUnassignedExecutors(topology).size() > 0;
    }

    /**
     * Gets a executor -> component-id map which needs scheduling in this topology.
     * 
     * @param topology
     * @return
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
     * Gets a component-id -> executors map which needs scheduling in this topology.
     * 
     * @param topology
     * @return
     */
    public Map<String, List<ExecutorDetails>> getNeedsSchedulingComponentToExecutors(TopologyDetails topology) {
        Map<ExecutorDetails, String> executorToComponents = this.getNeedsSchedulingExecutorToComponents(topology);
        Map<String, List<ExecutorDetails>> componentToExecutors = new HashMap<String, List<ExecutorDetails>>();
        for (ExecutorDetails executor : executorToComponents.keySet()) {
            String component = executorToComponents.get(executor);
            if (!componentToExecutors.containsKey(component)) {
                componentToExecutors.put(component, new ArrayList<ExecutorDetails>());
            }
            
            componentToExecutors.get(component).add(executor);
        }
        
        return componentToExecutors;
    }


    /**
     * Get all the used ports of this supervisor.
     * 
     * @param cluster
     * @return
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
     * 
     * @param cluster
     * @return
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
     * 
     * @param cluster
     * @return
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
     * Gets the number of workers assigned to this topology.
     * 
     * @param topology
     * @return
     */
    public int getAssignedNumWorkers(TopologyDetails topology) {
        SchedulerAssignment assignment = this.getAssignmentById(topology.getId());
        if (topology == null || assignment == null) {
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
     * Gets all the available slots in the cluster.
     * 
     * @return
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
     * Checks the specified slot is occupied.
     * 
     * @param slot the slot be to checked.
     * @return
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
     * Get all the supervisors.
     */
    public Map<String, SupervisorDetails> getSupervisors() {
        return this.supervisors;
    }
}
