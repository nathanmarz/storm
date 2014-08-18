package org.apache.storm.hdfs.trident;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.List;

public class HdfsUpdater extends BaseStateUpdater<HdfsState>{
    @Override
    public void updateState(HdfsState state, List<TridentTuple> tuples, TridentCollector collector) {
        state.updateState(tuples, collector);
    }
}
