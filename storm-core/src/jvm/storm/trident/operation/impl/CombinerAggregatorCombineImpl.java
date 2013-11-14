package storm.trident.operation.impl;

import backtype.storm.tuple.Values;
import java.util.Map;
import storm.trident.operation.Aggregator;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class CombinerAggregatorCombineImpl implements Aggregator<Result> {
    CombinerAggregator _agg;
    
    public CombinerAggregatorCombineImpl(CombinerAggregator agg) {
        _agg = agg;
    }
    
    public void prepare(Map conf, TridentOperationContext context) {
        
    }
    
    public Result init(Object batchId, TridentCollector collector) {
        Result ret = new Result();
        ret.obj = _agg.zero();
        return ret;
    }
    
    public void aggregate(Result val, TridentTuple tuple, TridentCollector collector) {
        Object v = tuple.getValue(0);
        if(val.obj==null) {
            val.obj = v;
        } else {
            val.obj = _agg.combine(val.obj, v);
        }
    }
    
    public void complete(Result val, TridentCollector collector) {
        collector.emit(new Values(val.obj));        
    }
    
    public void cleanup() {
        
    }
}
