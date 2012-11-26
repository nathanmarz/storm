package storm.trident.state;

import java.util.List;
import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;

public class ReducerValueUpdater<T> implements ValueUpdater<T> {
    List<TridentTuple> tuples;
    ReducerAggregator<T> agg;

    public ReducerValueUpdater(ReducerAggregator<T> agg, List<TridentTuple> tuples) {
        this.agg = agg;
        this.tuples = tuples;
    }

    @Override
    public T update(T stored) {
        T ret = stored;
        for(TridentTuple t: tuples) {
           ret =  this.agg.reduce(ret, t);
        }
        return ret;
    }
}