package storm.trident.state;

import java.util.List;
import storm.trident.operation.EachOperation;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public interface QueryFunction<S extends State, T> extends EachOperation {
    List<T> batchRetrieve(S state, List<TridentTuple> args);
    void execute(TridentTuple tuple, T result, TridentCollector collector);
}
