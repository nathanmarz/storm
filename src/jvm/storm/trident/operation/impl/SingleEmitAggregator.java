package storm.trident.operation.impl;

import java.io.Serializable;
import java.util.Map;
import storm.trident.operation.Aggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.operation.impl.SingleEmitAggregator.SingleEmitState;
import storm.trident.tuple.TridentTuple;


public class SingleEmitAggregator implements Aggregator<SingleEmitState> {
    public static interface BatchToPartition extends Serializable {
        int partitionIndex(Object batchId, int numPartitions);
    }
    
    static class SingleEmitState {
        boolean received = false;
        Object state;
        Object batchId;
        
        public SingleEmitState(Object batchId) {
            this.batchId = batchId;
        }
    }
    
    Aggregator _agg;
    BatchToPartition _batchToPartition;
    
    public SingleEmitAggregator(Aggregator agg, BatchToPartition batchToPartition) {
        _agg = agg;
        _batchToPartition = batchToPartition;
    }
    
    
    @Override
    public SingleEmitState init(Object batchId, TridentCollector collector) {
        return new SingleEmitState(batchId);
    }

    @Override
    public void aggregate(SingleEmitState val, TridentTuple tuple, TridentCollector collector) {
        if(!val.received) {
            val.state = _agg.init(val.batchId, collector);
            val.received = true;
        }
        _agg.aggregate(val.state, tuple, collector);
    }

    @Override
    public void complete(SingleEmitState val, TridentCollector collector) {
        if(!val.received) {
            if(this.myPartitionIndex == _batchToPartition.partitionIndex(val.batchId, this.totalPartitions)) {
                val.state = _agg.init(val.batchId, collector);
                _agg.complete(val.state, collector);
            }
        } else {
            _agg.complete(val.state, collector);
        }
    }

    int myPartitionIndex;
    int totalPartitions;
    
    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        _agg.prepare(conf, context);
        this.myPartitionIndex = context.getPartitionIndex();
        this.totalPartitions = context.numPartitions();
    }

    @Override
    public void cleanup() {
        _agg.cleanup();
    }
    
    
}
