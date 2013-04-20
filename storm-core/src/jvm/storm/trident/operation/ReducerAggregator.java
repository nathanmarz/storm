package storm.trident.operation;

import java.io.Serializable;
import storm.trident.tuple.TridentTuple;

public interface ReducerAggregator<T> extends Serializable {
    T init();
    T reduce(T curr, TridentTuple tuple);
}
