package storm.trident.operation.impl;


public class GlobalBatchToPartition implements SingleEmitAggregator.BatchToPartition {

    @Override
    public int partitionIndex(Object batchId, int numPartitions) {
        // TODO: take away knowledge of storm's internals here
        return 0;
    }
    
}
