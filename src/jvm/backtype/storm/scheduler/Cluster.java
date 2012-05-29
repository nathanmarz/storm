package backtype.storm.scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import backtype.storm.Config;

public class Cluster {

    /**
     * key: supervisor id, value: supervisor details
     */
    private Map<String, SupervisorDetails>   supervisors;
    /**
     * key: topologyId, value: topology's current assignments.
     */
    private Map<String, SchedulerAssignment> assignments;

    /**
     * a map from hostname to supervisor id.
     */
    private Map<String, List<String>>        hostToIds;

    public Cluster(Map<String, SupervisorDetails> supervisors, Map<String, SchedulerAssignment> assignments){
        this.supervisors = new HashMap<String, SupervisorDetails>(supervisors.size());
        this.supervisors.putAll(supervisors);
        this.assignments = new HashMap<String, SchedulerAssignment>(assignments.size());
        this.assignments.putAll(assignments);
        this.hostToIds = new HashMap<String, List<String>>();
        for (String nodeId : supervisors.keySet()) {
            SupervisorDetails supervisor = supervisors.get(nodeId);
            String host = supervisor.getHost();
            if (!this.supervisors.containsKey(host)) {
                this.hostToIds.put(host, new ArrayList<String>());
            }
            this.hostToIds.get(host).add(nodeId);
        }
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
     *   <li>There are unassigned tasks in this topology</li>
     * </ul>
     */
    public boolean needsScheduling(TopologyDetails topology) {
        int desiredNumWorkers = ((Number) topology.getConf().get(Config.TOPOLOGY_WORKERS)).intValue();
        int assignedNumWorkers = this.getAssignedNumWorkers(topology);

        if (desiredNumWorkers > assignedNumWorkers) {
            return true;
        }

        return this.getUnassignedTasks(topology).size() > 0;
    }

    /**
     * Gets a task-id -> component-id map which needs scheduling in this topology.
     * 
     * @param topology
     * @return
     */
    public Map<Integer, String> getNeedsSchedulingTaskToComponents(TopologyDetails topology) {
        Collection<Integer> allTasks = topology.getTasks();

        SchedulerAssignment assignment = this.assignments.get(topology.getId());
        if (assignment != null) {
            Collection<Integer> assignedTasks = assignment.getTasks();
            allTasks.removeAll(assignedTasks);
        }

        return topology.selectTaskToComponents(allTasks);
    }
    
    /**
     * Gets a component-id -> tasks map which needs scheduling in this topology.
     * 
     * @param topology
     * @return
     */
    public Map<String, List<Integer>> getNeedsSchedulingComponentToTasks(TopologyDetails topology) {
        Map<Integer, String> taskToComponents = this.getNeedsSchedulingTaskToComponents(topology);
        Map<String, List<Integer>> componentToTasks = new HashMap<String, List<Integer>>();
        for (int task : taskToComponents.keySet()) {
            String component = taskToComponents.get(task);
            if (!componentToTasks.containsKey(component)) {
                componentToTasks.put(component, new ArrayList<Integer>());
            }
            
            componentToTasks.get(component).add(task);
        }
        
        return componentToTasks;
    }


    /**
     * Get all the used ports of this supervisor.
     * 
     * @param cluster
     * @return
     */
    public List<Integer> getUsedPorts(SupervisorDetails supervisor) {
        Map<String, SchedulerAssignment> assignments = this.getAssignments();
        List<Integer> usedPorts = new ArrayList<Integer>();

        for (SchedulerAssignment assignment : assignments.values()) {
            for (WorkerSlot slot : assignment.getTaskToSlots().values()) {
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
    public List<Integer> getAvailablePorts(SupervisorDetails supervisor) {
        List<Integer> usedPorts = this.getUsedPorts(supervisor);

        List<Integer> ret = new ArrayList<Integer>();
        ret.addAll(supervisor.allPorts);
        ret.removeAll(usedPorts);

        return ret;
    }

    /**
     * Return all the available slots on this supervisor.
     * 
     * @param cluster
     * @return
     */
    public List<WorkerSlot> getAvailableSlots(SupervisorDetails supervisor) {
        List<Integer> ports = this.getAvailablePorts(supervisor);
        List<WorkerSlot> slots = new ArrayList<WorkerSlot>(ports.size());

        for (Integer port : ports) {
            slots.add(new WorkerSlot(supervisor.getId(), port));
        }

        return slots;
    }
    
    /**
     * get the unassigned tasks of the topology.
     */
    public List<Integer> getUnassignedTasks(TopologyDetails topology) {
        if (topology == null) {
            return new ArrayList<Integer>(0);
        }

        Collection<Integer> allTasks = topology.getTasks();
        SchedulerAssignment assignment = this.getAssignmentById(topology.getId());

        if (assignment != null) {
            Set<Integer> assignedTasks = assignment.getTasks();
            allTasks.removeAll(assignedTasks);
        }

        List<Integer> ret = new ArrayList<Integer>(allTasks.size());
        ret.addAll(allTasks);

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
        slots.addAll(assignment.getTaskToSlots().values());

        return slots.size();
    }

    /**
     * Assign the slot to the task for this topology.
     */
    public void assign(WorkerSlot slot, String topologyId, Collection<Integer> tasks) {
        SchedulerAssignment assignment = this.getAssignmentById(topologyId);

        if (assignment == null) {
            assignment = new SchedulerAssignment(topologyId, new HashMap<Integer, WorkerSlot>());
            this.assignments.put(topologyId, assignment);
        }

        assignment.assign(slot, tasks);
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

    /**
     * Free the specified slot.
     * 
     * @param slot
     */
    public void freeSlot(WorkerSlot slot) {
        SupervisorDetails supervisor = this.supervisors.get(slot.getNodeId());

        if (supervisor != null) {
            // remove the slot from the existing assignments
            for (SchedulerAssignment assignment : this.assignments.values()) {
                if (assignment.occupiedSlot(slot)) {
                    assignment.removeSlot(slot);
                    break;
                }
            }
        }
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

    /**
     * Get all the supervisors on the specified <code>host</code>.
     * 
     * @param host hostname of the supervisor
     * @return the <code>SupervisorDetails</code> object.
     */
    public List<SupervisorDetails> getSupervisorsByHost(String host) {
        List<String> nodeIds = this.hostToIds.get(host);
        List<SupervisorDetails> ret = new ArrayList<SupervisorDetails>();

        if (nodeIds != null) {
            for (String nodeId : nodeIds) {
                ret.add(this.getSupervisorById(nodeId));
            }
        }

        return ret;
    }

    /**
     * Set assignment for the specified topology.
     * 
     * @param topologyId the id of the topology the assignment is for.
     * @param assignment the assignment to be assigned.
     */
    public void setAssignmentById(String topologyId, SchedulerAssignment assignment) {
        this.assignments.put(topologyId, assignment);
    }

    /**
     * Get all the assignments.
     */
    public Map<String, SchedulerAssignment> getAssignments() {
        return this.assignments;
    }

    /**
     * Get all the supervisors.
     */
    public Map<String, SupervisorDetails> getSupervisors() {
        return this.supervisors;
    }
}
