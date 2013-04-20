package storm.trident.operation;

import backtype.storm.tuple.Fields;
import java.util.List;
import storm.trident.tuple.TridentTuple;
import storm.trident.tuple.TridentTupleView.ProjectionFactory;


public class TridentMultiReducerContext {
    List<TridentTuple.Factory> _factories;
    
    public TridentMultiReducerContext(List<TridentTuple.Factory> factories) {
        _factories = factories;        
    }
    
    public ProjectionFactory makeProjectionFactory(int streamIndex, Fields fields) {
        return new ProjectionFactory(_factories.get(streamIndex), fields);
    }    
}
