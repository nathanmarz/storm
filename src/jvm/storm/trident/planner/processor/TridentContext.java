package storm.trident.planner.processor;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.tuple.Fields;
import java.util.List;
import storm.trident.planner.TupleReceiver;
import storm.trident.tuple.TridentTuple.Factory;


public class TridentContext {
    Fields selfFields;
    List<Factory> parentFactories;
    List<String> parentStreams;
    List<TupleReceiver> receivers;
    String outStreamId;
    int stateIndex;
    BatchOutputCollector collector;
    
    public TridentContext(Fields selfFields, List<Factory> parentFactories,
            List<String> parentStreams, List<TupleReceiver> receivers, 
            String outStreamId, int stateIndex, BatchOutputCollector collector) {
        this.selfFields = selfFields;
        this.parentFactories = parentFactories;
        this.parentStreams = parentStreams;
        this.receivers = receivers;
        this.outStreamId = outStreamId;
        this.stateIndex = stateIndex;
        this.collector = collector;        
    }
    
    public List<Factory> getParentTupleFactories() {
        return parentFactories;
    }
    
    public Fields getSelfOutputFields() {
        return selfFields;
    }
    
    public List<String> getParentStreams() {
        return parentStreams;
    }
    
    public List<TupleReceiver> getReceivers() {
        return receivers;
    }
    
    public String getOutStreamId() {
        return outStreamId;
    }
    
    public int getStateIndex() {
        return stateIndex;
    }
    
    //for reporting errors
    public BatchOutputCollector getDelegateCollector() {
        return collector;
    }
}
