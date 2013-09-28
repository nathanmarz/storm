package storm.trident.operation.impl;

import java.util.Map;
import storm.trident.operation.Filter;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

// works by emitting null to the collector. since the planner knows this is an ADD node with
// no new output fields, it just passes the tuple forward
public class FilterExecutor implements Function {
    Filter _filter;

    public FilterExecutor(Filter filter) {
        _filter = filter;
    }
    
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        if(_filter.isKeep(tuple)) {
            collector.emit(null);
        }
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        _filter.prepare(conf, context);
    }

    @Override
    public void cleanup() {
        _filter.cleanup();
    }
    
}
