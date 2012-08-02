package storm.trident.operation;

import java.io.Serializable;
import java.util.Map;
import storm.trident.tuple.TridentTuple;


public interface GroupedMultiReducer<T> extends Serializable {
    void prepare(Map conf, TridentMultiReducerContext context);
    T init(TridentCollector collector, TridentTuple group);
    void execute(T state, int streamIndex, TridentTuple group, TridentTuple input, TridentCollector collector);
    void complete(T state, TridentTuple group, TridentCollector collector);
    void cleanup();
}
