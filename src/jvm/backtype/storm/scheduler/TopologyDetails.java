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
    Map<ExecutorDetails, String> executorToComponent;
    int numWorkers;
    Map meta;
 
    public TopologyDetails(String topologyId, Map topologyConf, StormTopology topology, Map meta, int numWorkers) {
        this.topologyId = topologyId;
        this.topologyConf = topologyConf;
        this.topology = topology;
        this.numWorkers = numWorkers;
        this.meta = meta;
    }
    
    public TopologyDetails(String topologyId, Map topologyConf, StormTopology topology, Map meta, int numWorkers, Map<ExecutorDetails, String> executorToComponents) {
        this(topologyId, topologyConf, topology, meta, numWorkers);
        this.executorToComponent = new HashMap<ExecutorDetails, String>(0);
        if (executorToComponents != null) {
            this.executorToComponent.putAll(executorToComponents);
        }
    }
    
    public String getId() {
        return topologyId;
    }
    
    public String getName() {
        return (String)this.topologyConf.get(Config.TOPOLOGY_NAME);
    }

    public Map getMeta() {
        return this.meta;
    }
    
    public Map getConf() {
        return topologyConf;
    }
    
    public int getNumWorkers() {
        return numWorkers;
    }
    
    public StormTopology getTopology() {
        return topology;
    }

    public Map<ExecutorDetails, String> getExecutorToComponent() {
        return this.executorToComponent;
    }

    public Map<ExecutorDetails, String> selectExecutorToComponent(Collection<ExecutorDetails> executors) {
        Map<ExecutorDetails, String> ret = new HashMap<ExecutorDetails, String>(executors.size());
        for (ExecutorDetails executor : executors) {
            String compId = this.executorToComponent.get(executor);
            if (compId != null) {
                ret.put(executor, compId);
            }
        }
        
        return ret;
    }
    
    public Collection<ExecutorDetails> getExecutors() {
        return this.executorToComponent.keySet();
    }
}
