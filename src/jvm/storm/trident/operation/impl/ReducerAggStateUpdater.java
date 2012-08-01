package storm.trident.operation.impl;

import backtype.storm.tuple.Values;
import java.util.List;
import java.util.Map;
import storm.trident.operation.ReducerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.ReducerValueUpdater;
import storm.trident.state.StateUpdater;
import storm.trident.state.snapshot.Snapshottable;
import storm.trident.tuple.TridentTuple;

public class ReducerAggStateUpdater implements StateUpdater<Snapshottable> {
    ReducerAggregator _agg;
    
    public ReducerAggStateUpdater(ReducerAggregator agg) {
        _agg = agg;
    }
    

    @Override
    public void updateState(Snapshottable state, List<TridentTuple> tuples, TridentCollector collector) {
        Object newVal = state.update(new ReducerValueUpdater(_agg, tuples));
        collector.emit(new Values(newVal));
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {        
    }

    @Override
    public void cleanup() {
    }
    
}
