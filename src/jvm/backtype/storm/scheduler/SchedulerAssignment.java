package backtype.storm.scheduler;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SchedulerAssignment {
    /**
     * topology-id this assignment is for.
     */
    String topologyId;
    /**
     * assignment detail, a mapping from executor to <code>WorkerSlot</code>
     */
    Map<ExecutorDetails, WorkerSlot> executorToSlots;
    
    public SchedulerAssignment(String topologyId, Map<ExecutorDetails, WorkerSlot> executorToSlots) {
        this.topologyId = topologyId;
        this.executorToSlots = new HashMap<ExecutorDetails, WorkerSlot>(0);
        if (executorToSlots != null) {
            this.executorToSlots.putAll(executorToSlots);
        }
    }
    
    /**
     * Assign the slot to executors.
     * @param slot
     * @param executors
     */
    public void assign(WorkerSlot slot, Collection<ExecutorDetails> executors) {
        for (ExecutorDetails executor : executors) {
            this.executorToSlots.put(executor, slot);
        }
    }
 
    /**
     * Release the slot occupied by this assignment.
     * @param slot
     */
    public void removeSlot(WorkerSlot slot) {
        for (ExecutorDetails executor : this.executorToSlots.keySet()) {
            WorkerSlot ws = this.executorToSlots.get(executor);
            if (ws.equals(slot)) {
                this.executorToSlots.remove(executor);
            }
        }
    }

    /**
     * Does this slot occupied by this assignment?
     * @param slot
     * @return
     */
    public boolean isSlotOccupied(WorkerSlot slot) {
        Collection<WorkerSlot> slots = this.executorToSlots.values();
        for (WorkerSlot slot1 : slots) {
            if (slot1.equals(slot)) {
                return true;
            }
        }

        return false;
    }

    public String getTopologyId() {
        return this.topologyId;
    }

    public Map<ExecutorDetails, WorkerSlot> getExecutorToSlots() {
        return this.executorToSlots;
    }

    /**
     * Return the executors covered by this assignments
     * @return
     */
    public Set<ExecutorDetails> getExecutors() {
        return this.executorToSlots.keySet();
    }
}