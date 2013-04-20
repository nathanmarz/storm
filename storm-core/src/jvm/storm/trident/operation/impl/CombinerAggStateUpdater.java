package storm.trident.operation.impl;

import backtype.storm.tuple.Values;
import java.util.List;
import java.util.Map;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.CombinerValueUpdater;
import storm.trident.state.StateUpdater;
import storm.trident.state.snapshot.Snapshottable;
import storm.trident.tuple.TridentTuple;

public class CombinerAggStateUpdater implements StateUpdater<Snapshottable> {
    CombinerAggregator _agg;
    
    public CombinerAggStateUpdater(CombinerAggregator agg) {
        _agg = agg;
    }
    

    @Override
    public void updateState(Snapshottable state, List<TridentTuple> tuples, TridentCollector collector) {
        if(tuples.size()!=1) {
            throw new IllegalArgumentException("Combiner state updater should receive a single tuple. Received: " + tuples.toString());
        }
        Object newVal = state.update(new CombinerValueUpdater(_agg, tuples.get(0).getValue(0)));
        collector.emit(new Values(newVal));
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {        
    }

    @Override
    public void cleanup() {
    }
    
}
