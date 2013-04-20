package storm.trident.operation;

import java.io.Serializable;
import java.util.Map;
import storm.trident.tuple.TridentTuple;


public interface MultiReducer<T> extends Serializable {
    void prepare(Map conf, TridentMultiReducerContext context);
    T init(TridentCollector collector);
    void execute(T state, int streamIndex, TridentTuple input, TridentCollector collector);
    void complete(T state, TridentCollector collector);
    void cleanup();
}
