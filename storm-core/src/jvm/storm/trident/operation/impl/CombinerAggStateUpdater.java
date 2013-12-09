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

import backtype.storm.tuple.Values;
import java.util.List;
import java.util.Map;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.CombinerValueUpdater;
import storm.trident.state.StateUpdater;
import storm.trident.state.snapshot.Snapshottable;
import storm.trident.tuple.TridentTuple;

public class CombinerAggStateUpdater implements StateUpdater<Snapshottable> {
    CombinerAggregator _agg;
    
    public CombinerAggStateUpdater(CombinerAggregator agg) {
        _agg = agg;
    }
    

    @Override
    public void updateState(Snapshottable state, List<TridentTuple> tuples, TridentCollector collector) {
        if(tuples.size()!=1) {
            throw new IllegalArgumentException("Combiner state updater should receive a single tuple. Received: " + tuples.toString());
        }
        Object newVal = state.update(new CombinerValueUpdater(_agg, tuples.get(0).getValue(0)));
        collector.emit(new Values(newVal));
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {        
    }

    @Override
    public void cleanup() {
    }
    
}
