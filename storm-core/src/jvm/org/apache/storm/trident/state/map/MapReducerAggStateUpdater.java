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
package org.apache.storm.trident.state.map;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.trident.operation.ReducerAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.state.ReducerValueUpdater;
import org.apache.storm.trident.state.StateUpdater;
import org.apache.storm.trident.state.ValueUpdater;
import org.apache.storm.trident.tuple.ComboList;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.tuple.TridentTupleView.ProjectionFactory;

public class MapReducerAggStateUpdater implements StateUpdater<MapState> {
    //ANY CHANGE TO THIS CODE MUST BE SERIALIZABLE COMPATIBLE OR THERE WILL BE PROBLEMS
    private static final long serialVersionUID = 8667174018978959987L;

    ReducerAggregator _agg;
    Fields _groupFields;
    Fields _inputFields;
    transient ProjectionFactory _groupFactory;
    transient ProjectionFactory _inputFactory;
    ComboList.Factory _factory;

    public MapReducerAggStateUpdater(ReducerAggregator agg, Fields groupFields, Fields inputFields) {
        _agg = agg;
        _groupFields = groupFields;
        _inputFields = inputFields;
        _factory = new ComboList.Factory(groupFields.size(), 1);
    }

    @Override
    public void updateState(MapState map, List<TridentTuple> tuples, TridentCollector collector) {
        Map<List<Object>, List<TridentTuple>> grouped = new HashMap<>();
        
        for(TridentTuple t: tuples) {
            List<Object> group = _groupFactory.create(t);
            List<TridentTuple> groupTuples = grouped.get(group);
            if(groupTuples==null) {
                groupTuples = new ArrayList<>();
                grouped.put(group, groupTuples);
            }
            groupTuples.add(_inputFactory.create(t));
        }
        List<List<Object>> uniqueGroups = new ArrayList<>(grouped.keySet());
        List<ValueUpdater> updaters = new ArrayList<>(uniqueGroups.size());
        for(List<Object> group: uniqueGroups) {
            updaters.add(new ReducerValueUpdater(_agg, grouped.get(group)));
        }
        List<Object> results = map.multiUpdate(uniqueGroups, updaters);

        for(int i=0; i<uniqueGroups.size(); i++) {
            List<Object> group = uniqueGroups.get(i);
            Object result = results.get(i);
            collector.emit(_factory.create(new List[] {group, new Values(result) }));
        }
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        _groupFactory = context.makeProjectionFactory(_groupFields);
        _inputFactory = context.makeProjectionFactory(_inputFields);
    }

    @Override
    public void cleanup() {
    }

}
