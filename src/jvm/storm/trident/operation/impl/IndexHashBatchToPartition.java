package storm.trident.operation.impl;

import storm.trident.partition.IndexHashGrouping;

public class IndexHashBatchToPartition implements SingleEmitAggregator.BatchToPartition {

    @Override
    public int partitionIndex(Object batchId, int numPartitions) {
        return IndexHashGrouping.objectToIndex(batchId, numPartitions);
    }
    
}
