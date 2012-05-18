package backtype.storm.scheduler;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;


public class TopologyDetails {
    String topologyId;
    Map topologyConf;
    StormTopology topology;
    Map<Integer, String> taskToComponents;
 
    public TopologyDetails(String topologyId, Map topologyConf, StormTopology topology) {
        this.topologyId = topologyId;
        this.topologyConf = topologyConf;
        this.topology = topology;
    }
    
    public TopologyDetails(String topologyId, Map topologyConf, StormTopology topology, Map<Integer, String> taskToComponents) {
        this.topologyId = topologyId;
        this.topologyConf = topologyConf;
        this.topology = topology;
        this.taskToComponents = new HashMap<Integer, String>(0);
        if (taskToComponents != null) {
            this.taskToComponents.putAll(taskToComponents);
        }
    }
    
    public String getId() {
        return topologyId;
    }
    
    public String getName() {
        return (String)this.topologyConf.get(Config.TOPOLOGY_NAME);
    }
    
    public Map getConf() {
        return topologyConf;
    }
    
    public StormTopology getTopology() {
        return topology;
    }

    public Map<Integer, String> getTaskToComponents() {
        return this.taskToComponents;
    }

    public Set<Integer> getTasks() {
        return this.taskToComponents.keySet();
    }
}
