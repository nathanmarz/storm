package backtype.storm.scheduler;

import backtype.storm.generated.StormTopology;
import java.util.Map;


public class TopologyDetails {
    String topologyId;
    Map topologyConf;
    StormTopology topology;
    
    public TopologyDetails(String topologyId, Map topologyConf, StormTopology topology) {
        this.topologyId = topologyId;
        this.topologyConf = topologyConf;
        this.topology = topology;
    }
    
    public String getId() {
        return topologyId;
    }
    
    public Map getConf() {
        return topologyConf;
    }
    
    public StormTopology getTopology() {
        return topology;
    }
}
