package backtype.storm.scheduler;

import java.util.Collection;
import java.util.Map;

public interface INimbus {
    void prepare(Map stormConf, String schedulerLocalDir);
    //used slots are slots that are currently assigned and haven't timed out
    // mesos should:
    //   1. if some slots are used, return as much as it currently has available
    //   2. otherwise return nothing until it has enough slots, or enough time has passed
    // sets the node id as {normalized hostname (invalid chars removed}-{topologyid}
    Collection<WorkerSlot> availableSlots(Collection<SupervisorDetails> existingSupervisors, Collection<WorkerSlot> usedSlots, Topologies topologies);
    // mesos should call launchTasks on an executor for this topology... 
    // gives it the executor with:
    //   - name: the node id
    // set the task id to {nodeid-port}
    // this should be called after the assignment is changed in ZK
    void assignSlots(Topologies topologies, Collection<WorkerSlot> newSlots);
    
    // map from node id to supervisor details
    String getHostName(Map<String, SupervisorDetails> existingSupervisors, String nodeId);
    
    IScheduler getForcedScheduler(); 
}
