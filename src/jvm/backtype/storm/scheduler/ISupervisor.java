package backtype.storm.scheduler;

import java.util.Map;


public interface ISupervisor {
    void prepare(Map stormConf, String schedulerLocalDir);
    boolean confirmAssigned(int port);
    Object getMetadata();
    
    // for mesos, this is {hostname}-{topologyid}
    String getId();
    // calls this before actually killing the worker locally...
    // sends a "task finished" update
    void killedWorker(int port);
}
