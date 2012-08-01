package storm.trident.operation.impl;

import java.util.Map;
import storm.trident.operation.MultiReducer;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentMultiReducerContext;
import storm.trident.tuple.TridentTuple;


public class IdentityMultiReducer implements MultiReducer {

    @Override
    public void prepare(Map conf, TridentMultiReducerContext context) {
    }

    @Override
    public Object init(TridentCollector collector) {
        return null;
    }

    @Override
    public void execute(Object state, int streamIndex, TridentTuple input, TridentCollector collector) {
        collector.emit(input);
    }

    @Override
    public void complete(Object state, TridentCollector collector) {
    }

    @Override
    public void cleanup() {
    }
    
}
