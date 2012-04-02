package backtype.storm.scheduler;

import backtype.storm.task.GeneralTopologyContext;
import java.util.Map;


public class TopologyInfo {
    String topologyId;
    Map topologyConf;
    GeneralTopologyContext context;
    
    public TopologyInfo(String topologyId, Map topologyConf, GeneralTopologyContext context) {
        this.topologyId = topologyId;
        this.topologyConf = topologyConf;
        this.context = context;
    }
    
    public String getId() {
        return topologyId;
    }
    
    public Map getConf() {
        return topologyConf;
    }
    
    public GeneralTopologyContext getContext() {
        return context;
    }
}
