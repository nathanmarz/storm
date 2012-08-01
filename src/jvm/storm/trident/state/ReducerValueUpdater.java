package storm.trident.state;

import java.util.List;
import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;

public class ReducerValueUpdater implements ValueUpdater<Object> {
    List<TridentTuple> tuples;
    ReducerAggregator agg;
    
    public ReducerValueUpdater(ReducerAggregator agg, List<TridentTuple> tuples) {
        this.agg = agg;
        this.tuples = tuples;
    }

    @Override
    public Object update(Object stored) {
        Object ret = stored;
        for(TridentTuple t: tuples) {
           ret =  this.agg.reduce(ret, t);
        }
        return ret;
    }        
}