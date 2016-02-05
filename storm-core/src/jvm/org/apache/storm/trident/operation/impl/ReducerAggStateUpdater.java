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
package org.apache.storm.trident.operation.impl;

import org.apache.storm.tuple.Values;
import java.util.List;
import java.util.Map;
import org.apache.storm.trident.operation.ReducerAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.state.ReducerValueUpdater;
import org.apache.storm.trident.state.StateUpdater;
import org.apache.storm.trident.state.snapshot.Snapshottable;
import org.apache.storm.trident.tuple.TridentTuple;

public class ReducerAggStateUpdater implements StateUpdater<Snapshottable> {
    ReducerAggregator _agg;
    
    public ReducerAggStateUpdater(ReducerAggregator agg) {
        _agg = agg;
    }
    

    @Override
    public void updateState(Snapshottable state, List<TridentTuple> tuples, TridentCollector collector) {
        Object newVal = state.update(new ReducerValueUpdater(_agg, tuples));
        collector.emit(new Values(newVal));
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {        
    }

    @Override
    public void cleanup() {
    }
    
}
