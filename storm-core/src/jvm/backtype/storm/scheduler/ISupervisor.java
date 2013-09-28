package backtype.storm.scheduler;

import java.util.Map;
import java.util.Collection;


public interface ISupervisor {
    void prepare(Map stormConf, String schedulerLocalDir);
    // for mesos, this is {hostname}-{topologyid}
    /**
     * The id used for writing metadata into ZK.
     */
    String getSupervisorId();
    /**
     * The id used in assignments. This combined with confirmAssigned decides what
     * this supervisor is responsible for. The combination of this and getSupervisorId
     * allows Nimbus to assign to a single machine and have multiple supervisors
     * on that machine execute the assignment. This is important for achieving resource isolation.
     */
    String getAssignmentId();
    Object getMetadata();
    
    boolean confirmAssigned(int port);
    // calls this before actually killing the worker locally...
    // sends a "task finished" update
    void killedWorker(int port);
    void assigned(Collection<Integer> ports);
}
