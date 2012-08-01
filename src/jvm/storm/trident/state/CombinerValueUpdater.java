package storm.trident.state;

import storm.trident.operation.CombinerAggregator;

public class CombinerValueUpdater implements ValueUpdater<Object> {
    Object arg;
    CombinerAggregator agg;
    
    public CombinerValueUpdater(CombinerAggregator agg, Object arg) {
        this.agg = agg;
        this.arg = arg;
    }

    @Override
    public Object update(Object stored) {
        if(stored==null) return arg;
        else return agg.combine(stored, arg);
    }
}
