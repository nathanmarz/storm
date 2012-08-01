package storm.trident.planner;

import backtype.storm.tuple.Fields;

public class ProcessorNode extends Node {
    
    public boolean committer; // for partitionpersist
    public TridentProcessor processor;
    public Fields selfOutFields;
    
    public ProcessorNode(String streamId, Fields allOutputFields, Fields selfOutFields, TridentProcessor processor) {
        super(streamId, allOutputFields);
        this.processor = processor;
        this.selfOutFields = selfOutFields;
    }
}
