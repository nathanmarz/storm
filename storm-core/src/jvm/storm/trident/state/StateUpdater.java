package storm.trident.state;

import java.util.List;
import storm.trident.operation.Operation;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;


public interface StateUpdater<S extends State> extends Operation {
    // maybe it needs a start phase (where it can do a retrieval, an update phase, and then a finish phase...?
    // shouldn't really be a one-at-a-time interface, since we have all the tuples already?
    // TOOD: used for the new values stream
    // the list is needed to be able to get reduceragg and combineragg persistentaggregate
    // for grouped streams working efficiently
    void updateState(S state, List<TridentTuple> tuples, TridentCollector collector);
}
