package storm.trident.operation;

import storm.trident.tuple.TridentTuple;

public interface Function extends EachOperation {
    void execute(TridentTuple tuple, TridentCollector collector);
}
