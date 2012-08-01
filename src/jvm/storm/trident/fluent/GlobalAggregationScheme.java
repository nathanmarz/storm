package storm.trident.fluent;

import storm.trident.operation.impl.SingleEmitAggregator.BatchToPartition;


public interface GlobalAggregationScheme<S extends IAggregatableStream> {
    IAggregatableStream aggPartition(S stream); // how to partition for second stage of aggregation
    BatchToPartition singleEmitPartitioner(); // return null if it's not single emit
}
