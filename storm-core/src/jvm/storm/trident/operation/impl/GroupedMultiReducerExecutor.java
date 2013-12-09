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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import storm.trident.operation.GroupedMultiReducer;
import storm.trident.operation.MultiReducer;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentMultiReducerContext;
import storm.trident.tuple.TridentTuple;
import storm.trident.tuple.TridentTupleView.ProjectionFactory;


public class GroupedMultiReducerExecutor implements MultiReducer<Map<TridentTuple, Object>> {
    GroupedMultiReducer _reducer;
    List<Fields> _groupFields;
    List<Fields> _inputFields;
    List<ProjectionFactory> _groupFactories = new ArrayList<ProjectionFactory>();
    List<ProjectionFactory> _inputFactories = new ArrayList<ProjectionFactory>();
    
    public GroupedMultiReducerExecutor(GroupedMultiReducer reducer, List<Fields> groupFields, List<Fields> inputFields) {
        if(inputFields.size()!=groupFields.size()) {
            throw new IllegalArgumentException("Multireducer groupFields and inputFields must be the same size");
        }
        _groupFields = groupFields;
        _inputFields = inputFields;
        _reducer = reducer;
    }
    
    @Override
    public void prepare(Map conf, TridentMultiReducerContext context) {
        for(int i=0; i<_groupFields.size(); i++) {
            _groupFactories.add(context.makeProjectionFactory(i, _groupFields.get(i)));
            _inputFactories.add(context.makeProjectionFactory(i, _inputFields.get(i)));
        }
        _reducer.prepare(conf, new TridentMultiReducerContext((List) _inputFactories));
    }

    @Override
    public Map<TridentTuple, Object> init(TridentCollector collector) {
        return new HashMap();
    }

    @Override
    public void execute(Map<TridentTuple, Object> state, int streamIndex, TridentTuple full, TridentCollector collector) {
        ProjectionFactory groupFactory = _groupFactories.get(streamIndex);
        ProjectionFactory inputFactory = _inputFactories.get(streamIndex);
        
        TridentTuple group = groupFactory.create(full);
        TridentTuple input = inputFactory.create(full);
        
        Object curr;
        if(!state.containsKey(group)) {
            curr = _reducer.init(collector, group);
            state.put(group, curr);
        } else {
            curr = state.get(group);
        }
        _reducer.execute(curr, streamIndex, group, input, collector);
    }

    @Override
    public void complete(Map<TridentTuple, Object> state, TridentCollector collector) {
        for(Map.Entry e: state.entrySet()) {
            TridentTuple group = (TridentTuple) e.getKey();
            Object val = e.getValue();
            _reducer.complete(val, group, collector);
        }
    }

    @Override
    public void cleanup() {
        _reducer.cleanup();
    }
    
}
