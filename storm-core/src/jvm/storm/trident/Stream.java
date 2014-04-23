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
package storm.trident;

import backtype.storm.generated.Grouping;
import backtype.storm.generated.NullStruct;
import storm.trident.fluent.ChainedAggregatorDeclarer;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import storm.trident.fluent.GlobalAggregationScheme;
import storm.trident.fluent.GroupedStream;
import storm.trident.fluent.IAggregatableStream;
import storm.trident.operation.Aggregator;
import storm.trident.operation.Assembly;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.Filter;
import storm.trident.operation.Function;
import storm.trident.operation.ReducerAggregator;
import storm.trident.operation.impl.CombinerAggStateUpdater;
import storm.trident.operation.impl.FilterExecutor;
import storm.trident.operation.impl.GlobalBatchToPartition;
import storm.trident.operation.impl.ReducerAggStateUpdater;
import storm.trident.operation.impl.IndexHashBatchToPartition;
import storm.trident.operation.impl.SingleEmitAggregator.BatchToPartition;
import storm.trident.operation.impl.TrueFilter;
import storm.trident.partition.GlobalGrouping;
import storm.trident.partition.IdentityGrouping;
import storm.trident.partition.IndexHashGrouping;
import storm.trident.planner.Node;
import storm.trident.planner.NodeStateInfo;
import storm.trident.planner.PartitionNode;
import storm.trident.planner.ProcessorNode;
import storm.trident.planner.processor.AggregateProcessor;
import storm.trident.planner.processor.EachProcessor;
import storm.trident.planner.processor.PartitionPersistProcessor;
import storm.trident.planner.processor.ProjectedProcessor;
import storm.trident.planner.processor.StateQueryProcessor;
import storm.trident.state.QueryFunction;
import storm.trident.state.StateFactory;
import storm.trident.state.StateSpec;
import storm.trident.state.StateUpdater;
import storm.trident.util.TridentUtils;

// TODO: need to be able to replace existing fields with the function fields (like Cascading Fields.REPLACE)
public class Stream implements IAggregatableStream {
    Node _node;
    TridentTopology _topology;
    String _name;
    
    protected Stream(TridentTopology topology, String name, Node node) {
        _topology = topology;
        _node = node;
        _name = name;
    }
    
    public Stream name(String name) {
        return new Stream(_topology, name, _node);
    }
    
    public Stream parallelismHint(int hint) {
        _node.parallelismHint = hint;
        return this;
    }
        
    public Stream project(Fields keepFields) {
        projectionValidation(keepFields);
        return _topology.addSourcedNode(this, new ProcessorNode(_topology.getUniqueStreamId(), _name, keepFields, new Fields(), new ProjectedProcessor(keepFields)));
    }

    public GroupedStream groupBy(Fields fields) {
        projectionValidation(fields);
        return new GroupedStream(this, fields);        
    }
    
    public Stream partitionBy(Fields fields) {
        projectionValidation(fields);
        return partition(Grouping.fields(fields.toList()));
    }
    
    public Stream partition(CustomStreamGrouping partitioner) {
        return partition(Grouping.custom_serialized(Utils.serialize(partitioner)));
    }
    
    public Stream shuffle() {
        return partition(Grouping.shuffle(new NullStruct()));
    }
    
    public Stream global() {
        // use this instead of storm's built in one so that we can specify a singleemitbatchtopartition
        // without knowledge of storm's internals
        return partition(new GlobalGrouping());
    }
    
    public Stream batchGlobal() {
        // the first field is the batch id
        return partition(new IndexHashGrouping(0));
    }
        
    public Stream broadcast() {
        return partition(Grouping.all(new NullStruct()));
    }
    
    public Stream identityPartition() {
        return partition(new IdentityGrouping());
    }
    
    public Stream partition(Grouping grouping) {
        if(_node instanceof PartitionNode) {
            return each(new Fields(), new TrueFilter()).partition(grouping);
        } else {
            return _topology.addSourcedNode(this, new PartitionNode(_node.streamId, _name, getOutputFields(), grouping));       
        }
    }
    
    public Stream applyAssembly(Assembly assembly) {
        return assembly.apply(this);
    }
    
    @Override
    public Stream each(Fields inputFields, Function function, Fields functionFields) {
        projectionValidation(inputFields);
        return _topology.addSourcedNode(this,
                new ProcessorNode(_topology.getUniqueStreamId(),
                    _name,
                    TridentUtils.fieldsConcat(getOutputFields(), functionFields),
                    functionFields,
                    new EachProcessor(inputFields, function)));
    }

    //creates brand new tuples with brand new fields
    @Override
    public Stream partitionAggregate(Fields inputFields, Aggregator agg, Fields functionFields) {
        projectionValidation(inputFields);
        return _topology.addSourcedNode(this,
                new ProcessorNode(_topology.getUniqueStreamId(),
                    _name,
                    functionFields,
                    functionFields,
                    new AggregateProcessor(inputFields, agg)));
    }
    
    public Stream stateQuery(TridentState state, Fields inputFields, QueryFunction function, Fields functionFields) {
        projectionValidation(inputFields);
        String stateId = state._node.stateInfo.id;
        Node n = new ProcessorNode(_topology.getUniqueStreamId(),
                        _name,
                        TridentUtils.fieldsConcat(getOutputFields(), functionFields),
                        functionFields,
                        new StateQueryProcessor(stateId, inputFields, function));
        _topology._colocate.get(stateId).add(n);
        return _topology.addSourcedNode(this, n);
    }
    
    public TridentState partitionPersist(StateFactory stateFactory, Fields inputFields, StateUpdater updater, Fields functionFields) {
      return partitionPersist(new StateSpec(stateFactory), inputFields, updater, functionFields);
    }
    
    public TridentState partitionPersist(StateSpec stateSpec, Fields inputFields, StateUpdater updater, Fields functionFields) {
        projectionValidation(inputFields);
        String id = _topology.getUniqueStateId();
        ProcessorNode n = new ProcessorNode(_topology.getUniqueStreamId(),
                    _name,
                    functionFields,
                    functionFields,
                    new PartitionPersistProcessor(id, inputFields, updater));
        n.committer = true;
        n.stateInfo = new NodeStateInfo(id, stateSpec);
        return _topology.addSourcedStateNode(this, n);
    }
    
    public TridentState partitionPersist(StateFactory stateFactory, Fields inputFields, StateUpdater updater) {
      return partitionPersist(stateFactory, inputFields, updater, new Fields());
    }
    
    public TridentState partitionPersist(StateSpec stateSpec, Fields inputFields, StateUpdater updater) {
      return partitionPersist(stateSpec, inputFields, updater, new Fields());        
    }
    
    public Stream each(Function function, Fields functionFields) {
        return each(null, function, functionFields);
    }
    
    public Stream each(Fields inputFields, Filter filter) {
        return each(inputFields, new FilterExecutor(filter), new Fields());
    }    
    
    public ChainedAggregatorDeclarer chainedAgg() {
        return new ChainedAggregatorDeclarer(this, new BatchGlobalAggScheme());
    }
    
    public Stream partitionAggregate(Aggregator agg, Fields functionFields) {
        return partitionAggregate(null, agg, functionFields);
    }

    public Stream partitionAggregate(CombinerAggregator agg, Fields functionFields) {
        return partitionAggregate(null, agg, functionFields);
    }

    public Stream partitionAggregate(Fields inputFields, CombinerAggregator agg, Fields functionFields) {
        projectionValidation(inputFields);
        return chainedAgg()
               .partitionAggregate(inputFields, agg, functionFields)
               .chainEnd();
    }  
    
    public Stream partitionAggregate(ReducerAggregator agg, Fields functionFields) {
        return partitionAggregate(null, agg, functionFields);
    }

    public Stream partitionAggregate(Fields inputFields, ReducerAggregator agg, Fields functionFields) {
        projectionValidation(inputFields);
        return chainedAgg()
               .partitionAggregate(inputFields, agg, functionFields)
               .chainEnd();
    }  
    
    public Stream aggregate(Aggregator agg, Fields functionFields) {
        return aggregate(null, agg, functionFields);
    }
    
    public Stream aggregate(Fields inputFields, Aggregator agg, Fields functionFields) {
        projectionValidation(inputFields);
        return chainedAgg()
               .aggregate(inputFields, agg, functionFields)
               .chainEnd();
    }

    public Stream aggregate(CombinerAggregator agg, Fields functionFields) {
        return aggregate(null, agg, functionFields);
    }

    public Stream aggregate(Fields inputFields, CombinerAggregator agg, Fields functionFields) {
        projectionValidation(inputFields);
        return chainedAgg()
               .aggregate(inputFields, agg, functionFields)
               .chainEnd();
    }

    public Stream aggregate(ReducerAggregator agg, Fields functionFields) {
        return aggregate(null, agg, functionFields);
    }

    public Stream aggregate(Fields inputFields, ReducerAggregator agg, Fields functionFields) {
        projectionValidation(inputFields);
        return chainedAgg()
                .aggregate(inputFields, agg, functionFields)
                .chainEnd();
    }
    
    public TridentState partitionPersist(StateFactory stateFactory, StateUpdater updater, Fields functionFields) {
        return partitionPersist(new StateSpec(stateFactory), updater, functionFields);
    }
    
    public TridentState partitionPersist(StateSpec stateSpec, StateUpdater updater, Fields functionFields) {
        return partitionPersist(stateSpec, null, updater, functionFields);
    }
    
    public TridentState partitionPersist(StateFactory stateFactory, StateUpdater updater) {
        return partitionPersist(stateFactory, updater, new Fields());
    }
    
    public TridentState partitionPersist(StateSpec stateSpec, StateUpdater updater) {
        return partitionPersist(stateSpec, updater, new Fields());
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
        projectionValidation(inputFields);
        // replaces normal aggregation here with a global grouping because it needs to be consistent across batches 
        return new ChainedAggregatorDeclarer(this, new GlobalAggScheme())
                .aggregate(inputFields, agg, functionFields)
                .chainEnd()
               .partitionPersist(spec, functionFields, new CombinerAggStateUpdater(agg), functionFields);
    }

    public TridentState persistentAggregate(StateFactory stateFactory, ReducerAggregator agg, Fields functionFields) {
        return persistentAggregate(new StateSpec(stateFactory), agg, functionFields);
    }

    public TridentState persistentAggregate(StateSpec spec, ReducerAggregator agg, Fields functionFields) {
        return persistentAggregate(spec, null, agg, functionFields);
    }

    public TridentState persistentAggregate(StateFactory stateFactory, Fields inputFields, ReducerAggregator agg, Fields functionFields) {
        return persistentAggregate(new StateSpec(stateFactory), inputFields, agg, functionFields);
    }

    public TridentState persistentAggregate(StateSpec spec, Fields inputFields, ReducerAggregator agg, Fields functionFields) {
        projectionValidation(inputFields);
        return global().partitionPersist(spec, inputFields, new ReducerAggStateUpdater(agg), functionFields);
    }
    
    public Stream stateQuery(TridentState state, QueryFunction function, Fields functionFields) {
        return stateQuery(state, null, function, functionFields);
    }

    @Override
    public Stream toStream() {
        return this;
    }

    @Override
    public Fields getOutputFields() {
        return _node.allOutputFields;
    }
    
    static class BatchGlobalAggScheme implements GlobalAggregationScheme<Stream> {

        @Override
        public IAggregatableStream aggPartition(Stream s) {
            return s.batchGlobal();
        }

        @Override
        public BatchToPartition singleEmitPartitioner() {
            return new IndexHashBatchToPartition();
        }
        
    }
    
    static class GlobalAggScheme implements GlobalAggregationScheme<Stream> {

        @Override
        public IAggregatableStream aggPartition(Stream s) {
            return s.global();
        }

        @Override
        public BatchToPartition singleEmitPartitioner() {
            return new GlobalBatchToPartition();
        }
        
    }

    private void projectionValidation(Fields projFields) {
        if (projFields == null) {
            return;
        }

        Fields allFields = this.getOutputFields();
        for (String field : projFields) {
            if (!allFields.contains(field)) {
                throw new IllegalArgumentException("Trying to select non-existent field: '" + field + "' from stream containing fields fields: <" + allFields + ">");
            }
        }
    }
}
