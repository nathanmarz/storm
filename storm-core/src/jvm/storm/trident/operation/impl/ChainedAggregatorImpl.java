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
package storm.trident.operation.impl;

import backtype.storm.tuple.Fields;
import java.util.List;
import java.util.Map;
import storm.trident.operation.Aggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.ComboList;
import storm.trident.tuple.TridentTuple;
import storm.trident.tuple.TridentTupleView;
import storm.trident.tuple.TridentTupleView.ProjectionFactory;

public class ChainedAggregatorImpl implements Aggregator<ChainedResult> {
    Aggregator[] _aggs;
    ProjectionFactory[] _inputFactories;
    ComboList.Factory _fact;
    Fields[] _inputFields;
    
    
    
    public ChainedAggregatorImpl(Aggregator[] aggs, Fields[] inputFields, ComboList.Factory fact) {
        _aggs = aggs;
        _inputFields = inputFields;
        _fact = fact;
        if(_aggs.length!=_inputFields.length) {
            throw new IllegalArgumentException("Require input fields for each aggregator");
        }
    }
    
    public void prepare(Map conf, TridentOperationContext context) {
        _inputFactories = new ProjectionFactory[_inputFields.length];
        for(int i=0; i<_inputFields.length; i++) {
            _inputFactories[i] = context.makeProjectionFactory(_inputFields[i]);
            _aggs[i].prepare(conf, new TridentOperationContext(context, _inputFactories[i]));
        }
    }
    
    public ChainedResult init(Object batchId, TridentCollector collector) {
        ChainedResult initted = new ChainedResult(collector, _aggs.length);
        for(int i=0; i<_aggs.length; i++) {
            initted.objs[i] = _aggs[i].init(batchId, initted.collectors[i]);
        }
        return initted;
    }
    
    public void aggregate(ChainedResult val, TridentTuple tuple, TridentCollector collector) {
        val.setFollowThroughCollector(collector);
        for(int i=0; i<_aggs.length; i++) {
            TridentTuple projected = _inputFactories[i].create((TridentTupleView) tuple);
            _aggs[i].aggregate(val.objs[i], projected, val.collectors[i]);
        }
    }
    
    public void complete(ChainedResult val, TridentCollector collector) {
        val.setFollowThroughCollector(collector);
        for(int i=0; i<_aggs.length; i++) {
            _aggs[i].complete(val.objs[i], val.collectors[i]);
        }
        if(_aggs.length > 1) { // otherwise, tuples were emitted directly
            int[] indices = new int[val.collectors.length];
            for(int i=0; i<indices.length; i++) {
                indices[i] = 0;
            }
            boolean keepGoing = true;
            //emit cross-join of all emitted tuples
            while(keepGoing) {
                List[] combined = new List[_aggs.length];
                for(int i=0; i< _aggs.length; i++) {
                    CaptureCollector capturer = (CaptureCollector) val.collectors[i];
                    combined[i] = capturer.captured.get(indices[i]);
                }
                collector.emit(_fact.create(combined));
                keepGoing = increment(val.collectors, indices, indices.length - 1);
            }
        }
    }
    
    //return false if can't increment anymore
    private boolean increment(TridentCollector[] lengths, int[] indices, int j) {
        if(j==-1) return false;
        indices[j]++;
        CaptureCollector capturer = (CaptureCollector) lengths[j];
        if(indices[j] >= capturer.captured.size()) {
            indices[j] = 0;
            return increment(lengths, indices, j-1);
        }
        return true;
    }
    
    public void cleanup() {
       for(Aggregator a: _aggs) {
           a.cleanup();
       } 
    } 
}
