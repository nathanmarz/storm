package backtype.storm.scheduler;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;


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

    public Map<Integer, String> selectTaskToComponents(Collection<Integer> tasks) {
        Map<Integer, String> ret = new HashMap<Integer, String>(tasks.size());
        for (int task : tasks) {
            String compId = this.taskToComponents.get(task);
            if (compId != null) {
                ret.put(task, compId);
            }
        }
        
        return ret;
    }
    
    public Collection<Integer> getTasks() {
        return this.taskToComponents.keySet();
    }
}
