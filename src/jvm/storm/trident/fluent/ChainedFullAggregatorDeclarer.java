package storm.trident.fluent;

import backtype.storm.tuple.Fields;
import storm.trident.operation.Aggregator;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.ReducerAggregator;

public interface ChainedFullAggregatorDeclarer extends IChainedAggregatorDeclarer {
    ChainedFullAggregatorDeclarer aggregate(Aggregator agg, Fields functionFields);
    ChainedFullAggregatorDeclarer aggregate(Fields inputFields, Aggregator agg, Fields functionFields);
    ChainedFullAggregatorDeclarer aggregate(CombinerAggregator agg, Fields functionFields);
    ChainedFullAggregatorDeclarer aggregate(Fields inputFields, CombinerAggregator agg, Fields functionFields);
    ChainedFullAggregatorDeclarer aggregate(ReducerAggregator agg, Fields functionFields);
    ChainedFullAggregatorDeclarer aggregate(Fields inputFields, ReducerAggregator agg, Fields functionFields);
}
