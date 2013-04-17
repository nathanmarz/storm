package storm.trident.operation;

import java.io.Serializable;
import storm.trident.tuple.TridentTuple;

// doesn't manipulate tuples (lists of stuff) so that things like aggregating into
// cassandra is cleaner (don't need lists everywhere, just store the single value there)
public interface CombinerAggregator<T> extends Serializable {
    T init(TridentTuple tuple);
    T combine(T val1, T val2);
    T zero();
}
