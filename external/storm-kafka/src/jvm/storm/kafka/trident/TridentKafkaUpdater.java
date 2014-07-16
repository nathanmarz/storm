package storm.kafka.trident;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.List;

public class TridentKafkaUpdater extends BaseStateUpdater<TridentKafkaState> {
    @Override
    public void updateState(TridentKafkaState state, List<TridentTuple> tuples, TridentCollector collector) {
        state.updateState(tuples, collector);
    }
}
