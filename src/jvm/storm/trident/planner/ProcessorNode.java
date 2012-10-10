package storm.trident.planner;

import backtype.storm.tuple.Fields;

public class ProcessorNode extends Node {
    
    public boolean committer; // for partitionpersist
    public TridentProcessor processor;
    public Fields selfOutFields;
    
    public ProcessorNode(String streamId, String name, Fields allOutputFields, Fields selfOutFields, TridentProcessor processor) {
        super(streamId, name, allOutputFields);
        this.processor = processor;
        this.selfOutFields = selfOutFields;
    }
}
