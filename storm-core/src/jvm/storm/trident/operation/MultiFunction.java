package storm.trident.operation;

import storm.trident.tuple.TridentTuple;
import storm.trident.planner.processor.MultiStreamCollector;

public interface MultiFunction extends EachOperation {
    void execute(TridentTuple tuple,  MultiStreamCollector collector);
}
