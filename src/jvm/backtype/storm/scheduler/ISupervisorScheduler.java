package backtype.storm.scheduler;

import java.util.Map;


public interface ISupervisorScheduler {
    void prepare(Map stormConf, String localDir);
    boolean isAssigned(int port);
    Object getMetadata();
}
