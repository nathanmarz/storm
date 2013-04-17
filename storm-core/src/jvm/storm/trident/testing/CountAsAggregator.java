package storm.trident.testing;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;


public class CountAsAggregator extends BaseAggregator<CountAsAggregator.State> {

    static class State {
        long count = 0;
    }
    
    @Override
    public State init(Object batchId, TridentCollector collector) {
        return new State();
    }

    @Override
    public void aggregate(State state, TridentTuple tuple, TridentCollector collector) {
        state.count++;
    }

    @Override
    public void complete(State state, TridentCollector collector) {
        collector.emit(new Values(state.count));
    }
    
}
