package backtype.storm;

import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.KillOptions;
import backtype.storm.generated.SubmitOptions;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.RebalanceOptions;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TopologyInfo;

import java.util.Map;


public interface ILocalCluster {
    void submitTopology(String topologyName, Map conf, StormTopology topology) throws AlreadyAliveException, InvalidTopologyException;
    void submitTopologyWithOpts(String topologyName, Map conf, StormTopology topology, SubmitOptions submitOpts) throws AlreadyAliveException, InvalidTopologyException;
    void killTopology(String topologyName) throws NotAliveException;
    void killTopologyWithOpts(String name, KillOptions options) throws NotAliveException;
    void activate(String topologyName) throws NotAliveException;
    void deactivate(String topologyName) throws NotAliveException;
    void rebalance(String name, RebalanceOptions options) throws NotAliveException;
    void shutdown();
    String getTopologyConf(String id);
    StormTopology getTopology(String id);
    ClusterSummary getClusterInfo();
    TopologyInfo getTopologyInfo(String id);
    Map getState();
}
