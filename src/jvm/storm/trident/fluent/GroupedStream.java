package storm.trident.fluent;

import backtype.storm.tuple.Fields;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.operation.Aggregator;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.Function;
import storm.trident.operation.ReducerAggregator;
import storm.trident.operation.impl.GroupedAggregator;
import storm.trident.operation.impl.SingleEmitAggregator.BatchToPartition;
import storm.trident.state.QueryFunction;
import storm.trident.state.StateFactory;
import storm.trident.state.StateSpec;
import storm.trident.state.map.MapCombinerAggStateUpdater;
import storm.trident.state.map.MapReducerAggStateUpdater;
import storm.trident.util.TridentUtils;


public class GroupedStream implements IAggregatableStream, GlobalAggregationScheme<GroupedStream> {
    Fields _groupFields;
    Stream _stream;
    
    public GroupedStream(Stream stream, Fields groupFields) {
        _groupFields = groupFields;
        _stream = stream;
    }
    
    public GroupedStream name(String name) {
        return new GroupedStream(_stream.name(name), _groupFields);
    }
    
    public ChainedAggregatorDeclarer chainedAgg() {
        return new ChainedAggregatorDeclarer(this, this);
    }
    
    public Stream aggregate(Aggregator agg, Fields functionFields) {
        return aggregate(null, agg, functionFields);
    }
    
    public Stream aggregate(Fields inputFields, Aggregator agg, Fields functionFields) {
        return new ChainedAggregatorDeclarer(this, this)
                .aggregate(inputFields, agg, functionFields)
                .chainEnd();
    }

    public Stream aggregate(CombinerAggregator agg, Fields functionFields) {
        return aggregate(null, agg, functionFields);
    }

    public Stream aggregate(Fields inputFields, CombinerAggregator agg, Fields functionFields) {
        return new ChainedAggregatorDeclarer(this, this)
                .aggregate(inputFields, agg, functionFields)
                .chainEnd();
    }

    public Stream aggregate(ReducerAggregator agg, Fields functionFields) {
        return aggregate(null, agg, functionFields);
    }

    public Stream aggregate(Fields inputFields, ReducerAggregator agg, Fields functionFields) {
        return new ChainedAggregatorDeclarer(this, this)
                .aggregate(inputFields, agg, functionFields)
                .chainEnd();
    }

    public TridentState persistentAggregate(StateFactory stateFactory, CombinerAggregator agg, Fields functionFields) {
        return persistentAggregate(new StateSpec(stateFactory), agg, functionFields);
    }

    public TridentState persistentAggregate(StateSpec spec, CombinerAggregator agg, Fields functionFields) {
        return persistentAggregate(spec, null, agg, functionFields);
    }

    public TridentState persistentAggregate(StateFactory stateFactory, Fields inputFields, CombinerAggregator agg, Fields functionFields) {
        return persistentAggregate(new StateSpec(stateFactory), inputFields, agg, functionFields);
    }

    public TridentState persistentAggregate(StateSpec spec, Fields inputFields, CombinerAggregator agg, Fields functionFields) {
        return aggregate(inputFields, agg, functionFields)
                .partitionPersist(spec,
                        TridentUtils.fieldsUnion(_groupFields, functionFields),
                        new MapCombinerAggStateUpdater(agg, _groupFields, functionFields),
                        TridentUtils.fieldsConcat(_groupFields, functionFields)); 
    }

    public TridentState persistentAggregate(StateFactory stateFactory, Fields inputFields, ReducerAggregator agg, Fields functionFields) {
        return persistentAggregate(new StateSpec(stateFactory), inputFields, agg, functionFields);
    }

    public TridentState persistentAggregate(StateSpec spec, Fields inputFields, ReducerAggregator agg, Fields functionFields) {
        return _stream.partitionBy(_groupFields)
                .partitionPersist(spec,
                    TridentUtils.fieldsUnion(_groupFields, inputFields),
                    new MapReducerAggStateUpdater(agg, _groupFields, inputFields),
                    TridentUtils.fieldsConcat(_groupFields, functionFields));
    }

    public Stream stateQuery(TridentState state, Fields inputFields, QueryFunction function, Fields functionFields) {
        return _stream.partitionBy(_groupFields)
                      .stateQuery(state,
                         inputFields,
                         function,
                         functionFields);
    }    

    public TridentState persistentAggregate(StateFactory stateFactory, ReducerAggregator agg, Fields functionFields) {
        return persistentAggregate(new StateSpec(stateFactory), agg, functionFields);
    }
    
    public TridentState persistentAggregate(StateSpec spec, ReducerAggregator agg, Fields functionFields) {
        return persistentAggregate(spec, null, agg, functionFields);
    }    
    
    public Stream stateQuery(TridentState state, QueryFunction function, Fields functionFields) {
        return stateQuery(state, null, function, functionFields);
    }
    
    @Override
    public IAggregatableStream each(Fields inputFields, Function function, Fields functionFields) {
        Stream s = _stream.each(inputFields, function, functionFields);
        return new GroupedStream(s, _groupFields);
    }

    @Override
    public IAggregatableStream partitionAggregate(Fields inputFields, Aggregator agg, Fields functionFields) {
        Aggregator groupedAgg = new GroupedAggregator(agg, _groupFields, inputFields, functionFields.size());
        Fields allInFields = TridentUtils.fieldsUnion(_groupFields, inputFields);
        Fields allOutFields = TridentUtils.fieldsConcat(_groupFields, functionFields);
        Stream s = _stream.partitionAggregate(allInFields, groupedAgg, allOutFields);
        return new GroupedStream(s, _groupFields);
    }

    @Override
    public IAggregatableStream aggPartition(GroupedStream s) {
        return new GroupedStream(s._stream.partitionBy(_groupFields), _groupFields);
    }

    @Override
    public Stream toStream() {
        return _stream;
    }

    @Override
    public Fields getOutputFields() {
        return _stream.getOutputFields();
    }    
    
    public Fields getGroupFields() {
        return _groupFields;
    }

    @Override
    public BatchToPartition singleEmitPartitioner() {
        return null;
    }
}
