package storm.trident.state;

import storm.trident.operation.CombinerAggregator;

public class CombinerValueUpdater<T> implements ValueUpdater<T> {
    T arg;
    CombinerAggregator<T> agg;

    public CombinerValueUpdater(CombinerAggregator<T> agg, T arg) {
        this.agg = agg;
        this.arg = arg;
    }

    @Override
    public T update(T stored) {
        if(stored==null) return arg;
        else return agg.combine(stored, arg);
    }
}
