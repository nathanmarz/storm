package storm.trident.fluent;

import backtype.storm.tuple.Fields;
import storm.trident.operation.Aggregator;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.ReducerAggregator;

public interface ChainedPartitionAggregatorDeclarer extends IChainedAggregatorDeclarer {
    ChainedPartitionAggregatorDeclarer partitionAggregate(Aggregator agg, Fields functionFields);
    ChainedPartitionAggregatorDeclarer partitionAggregate(Fields inputFields, Aggregator agg, Fields functionFields);
    ChainedPartitionAggregatorDeclarer partitionAggregate(CombinerAggregator agg, Fields functionFields);
    ChainedPartitionAggregatorDeclarer partitionAggregate(Fields inputFields, CombinerAggregator agg, Fields functionFields);
    ChainedPartitionAggregatorDeclarer partitionAggregate(ReducerAggregator agg, Fields functionFields);
    ChainedPartitionAggregatorDeclarer partitionAggregate(Fields inputFields, ReducerAggregator agg, Fields functionFields); 
}
