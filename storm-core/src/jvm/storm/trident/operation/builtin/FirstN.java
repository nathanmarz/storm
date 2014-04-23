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
package storm.trident.operation.builtin;

import backtype.storm.tuple.Fields;
import java.util.Comparator;
import java.util.PriorityQueue;
import storm.trident.Stream;
import storm.trident.operation.Aggregator;
import storm.trident.operation.Assembly;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;


public class FirstN implements Assembly {

    Aggregator _agg;
    
    public FirstN(int n, String sortField) {
        this(n, sortField, false);
    }
    
    public FirstN(int n, String sortField, boolean reverse) {
        if(sortField!=null) {
            _agg = new FirstNSortedAgg(n, sortField, reverse);
        } else {
            _agg = new FirstNAgg(n);
        }
    }
    
    @Override
    public Stream apply(Stream input) {
        Fields outputFields = input.getOutputFields();
        return input.partitionAggregate(outputFields, _agg, outputFields)
                    .global()
                    .partitionAggregate(outputFields, _agg, outputFields);             
    }
    
    public static class FirstNAgg extends BaseAggregator<FirstNAgg.State> {
        int _n;
        
        public FirstNAgg(int n) {
            _n = n;
        }
        
        static class State {
            int emitted = 0;
        }
        
        @Override
        public State init(Object batchId, TridentCollector collector) {
            return new State();
        }

        @Override
        public void aggregate(State val, TridentTuple tuple, TridentCollector collector) {
            if(val.emitted < _n) {
                collector.emit(tuple);
                val.emitted++;
            }
        }

        @Override
        public void complete(State val, TridentCollector collector) {
        }
        
    }
    
    public static class FirstNSortedAgg extends BaseAggregator<PriorityQueue> {

        int _n;
        String _sortField;
        boolean _reverse;
        
        public FirstNSortedAgg(int n, String sortField, boolean reverse) {
            _n = n;
            _sortField = sortField;
            _reverse = reverse;
        }

        @Override
        public PriorityQueue init(Object batchId, TridentCollector collector) {
            return new PriorityQueue(_n, new Comparator<TridentTuple>() {
                @Override
                public int compare(TridentTuple t1, TridentTuple t2) {
                    Comparable c1 = (Comparable) t1.getValueByField(_sortField);
                    Comparable c2 = (Comparable) t2.getValueByField(_sortField);
                    int ret = c1.compareTo(c2);
                    if(_reverse) ret *= -1;
                    return ret;
                }                
            });
        }

        @Override
        public void aggregate(PriorityQueue state, TridentTuple tuple, TridentCollector collector) {
            state.add(tuple);
        }

        @Override
        public void complete(PriorityQueue val, TridentCollector collector) {
            int total = val.size();
            for(int i=0; i<_n && i < total; i++) {
                TridentTuple t = (TridentTuple) val.remove();
                collector.emit(t);
            }
        }
    }    
}
