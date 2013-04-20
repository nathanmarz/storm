package storm.trident.fluent;

import backtype.storm.tuple.Fields;
import storm.trident.Stream;
import storm.trident.operation.Aggregator;
import storm.trident.operation.Function;
import storm.trident.operation.impl.SingleEmitAggregator.BatchToPartition;

public interface IAggregatableStream {
    IAggregatableStream each(Fields inputFields, Function function, Fields functionFields);
    IAggregatableStream partitionAggregate(Fields inputFields, Aggregator agg, Fields functionFields);
    Stream toStream();
    Fields getOutputFields();
}
