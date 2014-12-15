package org.apache.storm.hive.trident;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.List;

public class HiveUpdater extends BaseStateUpdater<HiveState>{
    @Override
    public void updateState(HiveState state, List<TridentTuple> tuples, TridentCollector collector) {
        state.updateState(tuples, collector);
    }
}
