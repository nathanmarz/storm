package backtype.storm.scheduler;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class Topologies {
    Map<String, TopologyDetails> topologies;
    Map<String, String> nameToIds;
    
    public Topologies(Map<String, TopologyDetails> topologies) {
        this.topologies = new HashMap<String, TopologyDetails>(topologies.size());
        this.topologies.putAll(topologies);
        this.nameToIds = new HashMap<String, String>(topologies.size());
        
        for (String topologyId : topologies.keySet()) {
            TopologyDetails topology = topologies.get(topologyId);
            this.nameToIds.put(topology.getName(), topologyId);
        }
    }
    
    public TopologyDetails getById(String topologyId) {
        return this.topologies.get(topologyId);
    }
    
    public TopologyDetails getByName(String topologyName) {
        String topologyId = this.nameToIds.get(topologyName);
        
        if (topologyId == null) {
            return null;
        } else {
            return this.getById(topologyId);
        }
    }
    
    public Collection<TopologyDetails> getTopologies() {
        return this.topologies.values();
    }
}
