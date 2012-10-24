package backtype.storm.scheduler;

import java.util.Map;
import java.util.Set;

public interface SchedulerAssignment {
    /**
     * Does this slot occupied by this assignment?
     * @param slot
     * @return
     */
    public boolean isSlotOccupied(WorkerSlot slot);

    /**
     * is the executor assigned?
     * 
     * @param executor
     * @return
     */
    public boolean isExecutorAssigned(ExecutorDetails executor);
    
    /**
     * get the topology-id this assignment is for.
     * @return
     */
    public String getTopologyId();

    /**
     * get the executor -> slot map.
     * @return
     */
    public Map<ExecutorDetails, WorkerSlot> getExecutorToSlot();

    /**
     * Return the executors covered by this assignments
     * @return
     */
    public Set<ExecutorDetails> getExecutors();
    
    public Set<WorkerSlot> getSlots();
}