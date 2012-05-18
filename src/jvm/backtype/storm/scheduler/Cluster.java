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
    public Collection<TopologyDetails> needsSchedulingTopologies(Topologies topologies) {
        List<TopologyDetails> ret = new ArrayList<TopologyDetails>();
        for (TopologyDetails topology : topologies.getTopologies()) {
            if (needsScheduling(topology)) {
                ret.add(topology);
            }
        }

        return ret;
    }

    /**
     * Does the topology need schedule?
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
     * get the unassigned tasks of the topology.
     */
    public List<Integer> getUnassignedTasks(TopologyDetails topology) {
        if (topology == null) {
            return new ArrayList<Integer>(0);
        }

        Set<Integer> allTasks = topology.getTasks();
        SchedulerAssignment assignment = this.getAssignmentById(topology.getId());

        if (assignment != null) {
            Set<Integer> assignedTasks = assignment.getTasks();
            allTasks.removeAll(assignedTasks);
        }

        List<Integer> ret = new ArrayList<Integer>(allTasks.size());
        ret.addAll(allTasks);

        return ret;
    }

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
     * assign the slot to the task for this topology.
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
    public Collection<WorkerSlot> getAvailableSlots(String topologyId) {
        List<WorkerSlot> slots = new ArrayList<WorkerSlot>();
        for (SupervisorDetails supervisor : this.supervisors.values()) {
            slots.addAll(supervisor.getAvailableSlots(this));
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
            // remove the port from the existing assignments
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
