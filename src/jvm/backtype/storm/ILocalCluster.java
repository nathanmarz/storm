package backtype.storm;

import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.StormTopology;
import java.util.Map;


public interface ILocalCluster {
    void submitTopology(String topologyName, Map conf, StormTopology topology)
            throws AlreadyAliveException, InvalidTopologyException;
    void killTopology(String topologyName)
            throws NotAliveException;
    void shutdown();
}
