package backtype.storm.scheduler;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public interface INimbus {
    void prepare(Map stormConf, String schedulerLocalDir);
    /**
     * Returns all slots that are available for the next round of scheduling. A slot is available for scheduling
     * if it is free and can be assigned to, or if it is used and can be reassigned.
     */
    Collection<WorkerSlot> allSlotsAvailableForScheduling(Collection<SupervisorDetails> existingSupervisors, Topologies topologies, Set<String> topologiesMissingAssignments);

    // this is called after the assignment is changed in ZK
    void assignSlots(Topologies topologies, Map<String, Collection<WorkerSlot>> newSlotsByTopologyId);
    
    // map from node id to supervisor details
    String getHostName(Map<String, SupervisorDetails> existingSupervisors, String nodeId);
    
    IScheduler getForcedScheduler(); 
}
