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
     * assignment detail, a mapping from task to <code>WorkerSlot</code>
     */
    Map<Integer, WorkerSlot> taskToSlots;
    
    public SchedulerAssignment(String topologyId, Map<Integer, WorkerSlot> taskToSlots) {
        this.topologyId = topologyId;
        this.taskToSlots = new HashMap<Integer, WorkerSlot>(0);
        if (taskToSlots != null) {
            this.taskToSlots.putAll(taskToSlots);
        }
    }
    
    /**
     * Assign the slot to tasks.
     * @param slot
     * @param tasks
     */
    public void assign(WorkerSlot slot, Collection<Integer> tasks) {
        for (Integer task : tasks) {
            this.taskToSlots.put(task, slot);
        }
    }
 
    /**
     * Release the slot occupied by this assignment.
     * @param slot
     */
    public void removeSlot(WorkerSlot slot) {
        for (int task : this.taskToSlots.keySet()) {
            WorkerSlot ws = this.taskToSlots.get(task);
            if (ws.equals(slot)) {
                this.taskToSlots.remove(task);
            }
        }
    }

    /**
     * Does this slot occupied by this assignment?
     * @param slot
     * @return
     */
    public boolean occupiedSlot(WorkerSlot slot) {
        Collection<WorkerSlot> slots = this.taskToSlots.values();
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

    public Map<Integer, WorkerSlot> getTaskToSlots() {
        return this.taskToSlots;
    }

    /**
     * Return the tasks covered by this assignments
     * @return
     */
    public Set<Integer> getTasks() {
        return this.taskToSlots.keySet();
    }
}