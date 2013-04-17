package backtype.storm.scheduler;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class Topologies {
    Map<String, TopologyDetails> topologies;
    Map<String, String> nameToId;
    
    public Topologies(Map<String, TopologyDetails> topologies) {
        if(topologies==null) topologies = new HashMap();
        this.topologies = new HashMap<String, TopologyDetails>(topologies.size());
        this.topologies.putAll(topologies);
        this.nameToId = new HashMap<String, String>(topologies.size());
        
        for (String topologyId : topologies.keySet()) {
            TopologyDetails topology = topologies.get(topologyId);
            this.nameToId.put(topology.getName(), topologyId);
        }
    }
    
    public TopologyDetails getById(String topologyId) {
        return this.topologies.get(topologyId);
    }
    
    public TopologyDetails getByName(String topologyName) {
        String topologyId = this.nameToId.get(topologyName);
        
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
