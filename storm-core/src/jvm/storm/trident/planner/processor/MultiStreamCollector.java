package storm.trident.planner.processor;


import java.util.List;
import storm.trident.planner.ProcessorContext;
import storm.trident.planner.TupleReceiver;
import storm.trident.tuple.TridentTuple;
import storm.trident.tuple.TridentTuple.Factory;
import storm.trident.tuple.MultiOutputFactory;
import storm.trident.tuple.TridentTupleView;
import storm.trident.tuple.TridentTupleView.OperationOutputFactory;

/**
   A collector for MultiFunction. Intentionally does not implement
   the trident collector interface. The user MUST specify a stream
   to route output to.
 */
public class MultiStreamCollector {

    MultiOutputFactory _factory;
    MultiOutputMapping _outputMap;    
    TridentContext _triContext;
    TridentTuple tuple;
    ProcessorContext context;
        
    public MultiStreamCollector(TridentContext context, MultiOutputMapping outputMap) {
        _triContext = context;
        _factory = new MultiOutputFactory(context.getParentTupleFactories().get(0), outputMap);        
        _triContext = context;
        _outputMap = outputMap;
    }
    
    public void setContext(ProcessorContext pc, TridentTuple t) {
        this.context = pc;
        this.tuple = t;
    }

    /**
       User interface. Routes values to the appropriate stream
     */
    public void emitTo(String name, List<Object> values) {
        OperationOutputFactory f = _factory.getFactory(name);
        TridentTuple toEmit = f.create((TridentTupleView)tuple, values);        
        _outputMap.getReceiver(name).execute(context, _triContext.getOutStreamId(), toEmit);
    }

    public void reportError(Throwable t) {
        _triContext.getDelegateCollector().reportError(t);
    } 
}
