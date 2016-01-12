/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.trident.fluent;

import org.apache.storm.tuple.Fields;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.operation.Aggregator;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.ReducerAggregator;
import org.apache.storm.trident.operation.impl.GroupedAggregator;
import org.apache.storm.trident.operation.impl.SingleEmitAggregator.BatchToPartition;
import org.apache.storm.trident.state.QueryFunction;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.StateSpec;
import org.apache.storm.trident.state.map.MapCombinerAggStateUpdater;
import org.apache.storm.trident.state.map.MapReducerAggStateUpdater;
import org.apache.storm.trident.util.TridentUtils;


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
