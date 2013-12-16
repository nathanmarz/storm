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
package storm.trident.fluent;

import backtype.storm.tuple.Fields;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import storm.trident.Stream;
import storm.trident.operation.Aggregator;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.ReducerAggregator;
import storm.trident.operation.impl.ChainedAggregatorImpl;
import storm.trident.operation.impl.CombinerAggregatorCombineImpl;
import storm.trident.operation.impl.CombinerAggregatorInitImpl;
import storm.trident.operation.impl.ReducerAggregatorImpl;
import storm.trident.operation.impl.SingleEmitAggregator;
import storm.trident.operation.impl.SingleEmitAggregator.BatchToPartition;
import storm.trident.tuple.ComboList;


public class ChainedAggregatorDeclarer implements ChainedFullAggregatorDeclarer, ChainedPartitionAggregatorDeclarer {    
    public static interface AggregationPartition {
        Stream partition(Stream input);
    }
    
    private static enum AggType {
        PARTITION,
        FULL,
        FULL_COMBINE
    }
    
    // inputFields can be equal to outFields, but multiple aggregators cannot have intersection outFields
    private static class AggSpec {
        Fields inFields;
        Aggregator agg;
        Fields outFields;
        
        public AggSpec(Fields inFields, Aggregator agg, Fields outFields) {
            this.inFields = inFields;
            this.agg = agg;
            this.outFields = outFields;
        }
    }
    
    List<AggSpec> _aggs = new ArrayList<AggSpec>();
    IAggregatableStream _stream;
    AggType _type = null;
    GlobalAggregationScheme _globalScheme;
    
    public ChainedAggregatorDeclarer(IAggregatableStream stream, GlobalAggregationScheme globalScheme) {
        _stream = stream;
        _globalScheme = globalScheme;
    }
    
    public Stream chainEnd() {
        Fields[] inputFields = new Fields[_aggs.size()];
        Aggregator[] aggs = new Aggregator[_aggs.size()];
        int[] outSizes = new int[_aggs.size()];
        List<String> allOutFields = new ArrayList<String>();
        Set<String> allInFields = new HashSet<String>();
        for(int i=0; i<_aggs.size(); i++) {
            AggSpec spec = _aggs.get(i);
            Fields infields = spec.inFields;
            if(infields==null) infields = new Fields();
            Fields outfields = spec.outFields;
            if(outfields==null) outfields = new Fields();

            inputFields[i] = infields;
            aggs[i] = spec.agg;
            outSizes[i] = outfields.size();  
            allOutFields.addAll(outfields.toList());
            allInFields.addAll(infields.toList());
        }
        if(new HashSet(allOutFields).size() != allOutFields.size()) {
            throw new IllegalArgumentException("Output fields for chained aggregators must be distinct: " + allOutFields.toString());
        }
        
        Fields inFields = new Fields(new ArrayList<String>(allInFields));
        Fields outFields = new Fields(allOutFields);
        Aggregator combined = new ChainedAggregatorImpl(aggs, inputFields, new ComboList.Factory(outSizes));
        
        if(_type!=AggType.FULL) {
            _stream = _stream.partitionAggregate(inFields, combined, outFields);
        }
        if(_type!=AggType.PARTITION) {
            _stream = _globalScheme.aggPartition(_stream);
            BatchToPartition singleEmit = _globalScheme.singleEmitPartitioner();
            Aggregator toAgg = combined;
            if(singleEmit!=null) {
                toAgg = new SingleEmitAggregator(combined, singleEmit);
            }
            // this assumes that inFields and outFields are the same for combineragg
            // assumption also made above
            _stream = _stream.partitionAggregate(inFields, toAgg, outFields);
        }
        return _stream.toStream();
    }

    public ChainedPartitionAggregatorDeclarer partitionAggregate(Aggregator agg, Fields functionFields) {
        return partitionAggregate(null, agg, functionFields);
    }

    public ChainedPartitionAggregatorDeclarer partitionAggregate(Fields inputFields, Aggregator agg, Fields functionFields) {
        _type = AggType.PARTITION;
        _aggs.add(new AggSpec(inputFields, agg, functionFields));
        return this;
    }

    public ChainedPartitionAggregatorDeclarer partitionAggregate(CombinerAggregator agg, Fields functionFields) {
        return partitionAggregate(null, agg, functionFields);
    }

    public ChainedPartitionAggregatorDeclarer partitionAggregate(Fields inputFields, CombinerAggregator agg, Fields functionFields) {
        initCombiner(inputFields, agg, functionFields);
        return partitionAggregate(functionFields, new CombinerAggregatorCombineImpl(agg), functionFields);
    }  
    
    public ChainedPartitionAggregatorDeclarer partitionAggregate(ReducerAggregator agg, Fields functionFields) {
        return partitionAggregate(null, agg, functionFields);
    }

    public ChainedPartitionAggregatorDeclarer partitionAggregate(Fields inputFields, ReducerAggregator agg, Fields functionFields) {
        return partitionAggregate(inputFields, new ReducerAggregatorImpl(agg), functionFields);
    }  
    
    public ChainedFullAggregatorDeclarer aggregate(Aggregator agg, Fields functionFields) {
        return aggregate(null, agg, functionFields);
    }
    
    public ChainedFullAggregatorDeclarer aggregate(Fields inputFields, Aggregator agg, Fields functionFields) {
        return aggregate(inputFields, agg, functionFields, false);
    }
    
    private ChainedFullAggregatorDeclarer aggregate(Fields inputFields, Aggregator agg, Fields functionFields, boolean isCombiner) {
        if(isCombiner) {
            if(_type == null) {
                _type = AggType.FULL_COMBINE;            
            }
        } else {
            _type = AggType.FULL;
        }
        _aggs.add(new AggSpec(inputFields, agg, functionFields));
        return this;
    }

    public ChainedFullAggregatorDeclarer aggregate(CombinerAggregator agg, Fields functionFields) {
        return aggregate(null, agg, functionFields);
    }

    public ChainedFullAggregatorDeclarer aggregate(Fields inputFields, CombinerAggregator agg, Fields functionFields) {
        initCombiner(inputFields, agg, functionFields);
        return aggregate(functionFields, new CombinerAggregatorCombineImpl(agg), functionFields, true);
    }

    public ChainedFullAggregatorDeclarer aggregate(ReducerAggregator agg, Fields functionFields) {
        return aggregate(null, agg, functionFields);
    }

    public ChainedFullAggregatorDeclarer aggregate(Fields inputFields, ReducerAggregator agg, Fields functionFields) {
        return aggregate(inputFields, new ReducerAggregatorImpl(agg), functionFields);
    }
    
    private void initCombiner(Fields inputFields, CombinerAggregator agg, Fields functionFields) {
        _stream = _stream.each(inputFields, new CombinerAggregatorInitImpl(agg), functionFields);        
    }
}
