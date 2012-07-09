package backtype.storm.scheduler;

import java.util.Map;
import java.util.Collection;


public interface ISupervisor {
    void prepare(Map stormConf, String schedulerLocalDir);
    // for mesos, this is {hostname}-{topologyid}
    String getId();
    Object getMetadata();
    
    boolean confirmAssigned(int port);
    // calls this before actually killing the worker locally...
    // sends a "task finished" update
    void killedWorker(int port);
    void assigned(Collection<Integer> ports);
}
