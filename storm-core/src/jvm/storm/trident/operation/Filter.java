package storm.trident.operation;

import storm.trident.tuple.TridentTuple;


public interface Filter extends EachOperation {
    boolean isKeep(TridentTuple tuple);
}
