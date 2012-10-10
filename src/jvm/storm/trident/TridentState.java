package storm.trident;

import storm.trident.planner.Node;


public class TridentState {
    TridentTopology _topology;
    Node _node;
    
    protected TridentState(TridentTopology topology, Node node) {
        _topology = topology;
        _node = node;
    }
    
    public Stream newValuesStream() {
        return new Stream(_topology, _node.name, _node);
    }
    
    public TridentState parallelismHint(int parallelism) {
        _node.parallelismHint = parallelism;
        return this;
    }
}
