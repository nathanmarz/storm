package storm.trident.operation.impl;

import backtype.storm.tuple.Values;
import java.util.Map;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class CombinerAggregatorInitImpl implements Function {

    CombinerAggregator _agg;
    
    public CombinerAggregatorInitImpl(CombinerAggregator agg) {
        _agg = agg;
    }
    
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        collector.emit(new Values(_agg.init(tuple)));
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
    }

    @Override
    public void cleanup() {
    }
    
}
