package backtype.storm.scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class SupervisorDetails {

    String        id;
    /**
     * hostname of this supervisor
     */
    String        host;
    Object        meta;
    /**
     * meta data configured for this supervisor
     */
    Object        schedulerMeta;
    /**
     * all the ports of the supervisor
     */
    Collection<Integer> allPorts;

    public SupervisorDetails(String id, Object meta){
        this.id = id;
        this.meta = meta;
    }

    public SupervisorDetails(String id, String host, Object schedulerMeta, Collection<Integer> allPorts){
        this.id = id;
        this.host = host;
        this.schedulerMeta = schedulerMeta;

        this.allPorts = new ArrayList<Integer>();
        if (allPorts != null && !allPorts.isEmpty()) {
            this.allPorts.addAll(allPorts);
        }
    }

    public String getId() {
        return id;
    }

    public String getHost() {
        return host;
    }

    public Object getMeta() {
        return meta;
    }

    public Object getSchedulerMeta() {
        return this.schedulerMeta;
    }

    /**
     * Get all the used ports of this supervisor.
     * 
     * @param cluster
     * @return
     */
    public List<Integer> getUsedPorts(Cluster cluster) {
        Map<String, SchedulerAssignment> assignments = cluster.getAssignments();
        List<Integer> usedPorts = new ArrayList<Integer>();

        for (SchedulerAssignment assignment : assignments.values()) {
            for (WorkerSlot slot : assignment.getTaskToSlots().values()) {
                if (slot.getNodeId().equals(this.id)) {
                    usedPorts.add(slot.getPort());
                }
            }
        }

        return usedPorts;
    }

    /**
     * return all the ports.
     * 
     * @return
     */
    public Collection<Integer> getAllPorts() {
        return this.allPorts;
    }

    /**
     * Return the available ports of this supervisor.
     * 
     * @param cluster
     * @return
     */
    public List<Integer> getAvailablePorts(Cluster cluster) {
        List<Integer> usedPorts = this.getUsedPorts(cluster);

        List<Integer> ret = new ArrayList<Integer>();
        ret.addAll(this.allPorts);
        ret.removeAll(usedPorts);

        return ret;
    }

    /**
     * Return all the available slots on this supervisor.
     * 
     * @param cluster
     * @return
     */
    public List<WorkerSlot> getAvailableSlots(Cluster cluster) {
        List<Integer> ports = this.getAvailablePorts(cluster);
        List<WorkerSlot> slots = new ArrayList<WorkerSlot>(ports.size());

        for (Integer port : ports) {
            slots.add(new WorkerSlot(this.id, port));
        }

        return slots;
    }
}
