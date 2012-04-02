package backtype.storm.scheduler;

import java.util.Map;


public interface ISupervisor {
    void prepare(Map stormConf, String localDir);
    boolean isAssigned(int port);
    Object getMetadata();
    
    // for mesos, this is {slaveid}-{topologyid} (use ExecutorArgs#getData for this)
    String getId();
}
