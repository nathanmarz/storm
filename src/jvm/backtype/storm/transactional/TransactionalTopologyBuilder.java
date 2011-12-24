package backtype.storm.transactional;

import backtype.storm.topology.InputDeclarer;

public class TransactionalTopologyBuilder {
    //TODO: finish
    
    public TransactionalTopologyBuilder(String spoutId, ITransactionalSpout spout) {
        
    }
    
    public InputDeclarer setBolt(String id, ITransactionalBolt bolt) {
        return null;
    }
}
