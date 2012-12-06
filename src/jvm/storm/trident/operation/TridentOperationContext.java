package storm.trident.operation;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import storm.trident.tuple.TridentTuple;
import storm.trident.tuple.TridentTupleView.ProjectionFactory;

public class TridentOperationContext {
    TridentTuple.Factory _factory;
    TopologyContext _topoContext;
    
    public TridentOperationContext(TopologyContext topoContext, TridentTuple.Factory factory) {
        _factory = factory;
        _topoContext = topoContext;
    }
    
    public TridentOperationContext(TridentOperationContext parent, TridentTuple.Factory factory) {
        this(parent._topoContext, factory);
    }    
    
    public ProjectionFactory makeProjectionFactory(Fields fields) {
        return new ProjectionFactory(_factory, fields);
    }
    
    public int numPartitions() {
        return _topoContext.getComponentTasks(_topoContext.getThisComponentId()).size();
    }
    
    public int getPartitionIndex() {
        return _topoContext.getThisTaskIndex();
    }

    public String getCodeDir() {
        return _topoContext.getCodeDir();
    }
}
