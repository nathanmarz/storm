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
package org.apache.storm.trident.planner.processor;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.planner.ProcessorContext;
import org.apache.storm.trident.planner.TridentProcessor;
import org.apache.storm.trident.state.QueryFunction;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.tuple.TridentTuple.Factory;
import org.apache.storm.trident.tuple.TridentTupleView.ProjectionFactory;


public class StateQueryProcessor implements TridentProcessor {
    QueryFunction _function;
    State _state;
    String _stateId;
    TridentContext _context;
    Fields _inputFields;
    ProjectionFactory _projection;
    AppendCollector _collector;
    
    public StateQueryProcessor(String stateId, Fields inputFields, QueryFunction function) {
        _stateId = stateId;
        _function = function;
        _inputFields = inputFields;
    }
    
    @Override
    public void prepare(Map conf, TopologyContext context, TridentContext tridentContext) {
        List<Factory> parents = tridentContext.getParentTupleFactories();
        if(parents.size()!=1) {
            throw new RuntimeException("State query operation can only have one parent");
        }
        _context = tridentContext;
        _state = (State) context.getTaskData(_stateId);
        _projection = new ProjectionFactory(parents.get(0), _inputFields);
        _collector = new AppendCollector(tridentContext);
        _function.prepare(conf, new TridentOperationContext(context, _projection));
    }

    @Override
    public void cleanup() {
        _function.cleanup();
    }

    @Override
    public void startBatch(ProcessorContext processorContext) {
        processorContext.state[_context.getStateIndex()] =  new BatchState();
    }

    @Override
    public void execute(ProcessorContext processorContext, String streamId, TridentTuple tuple) {
        BatchState state = (BatchState) processorContext.state[_context.getStateIndex()];
        state.tuples.add(tuple);
        state.args.add(_projection.create(tuple));
    }

    @Override
    public void finishBatch(ProcessorContext processorContext) {
        BatchState state = (BatchState) processorContext.state[_context.getStateIndex()];
        if(!state.tuples.isEmpty()) {
            List<Object> results = _function.batchRetrieve(_state, Collections.unmodifiableList(state.args));
            if(results.size()!=state.tuples.size()) {
                throw new RuntimeException("Results size is different than argument size: " + results.size() + " vs " + state.tuples.size());
            }
            for(int i=0; i<state.tuples.size(); i++) {
                TridentTuple tuple = state.tuples.get(i);
                Object result = results.get(i);
                _collector.setContext(processorContext, tuple);
                _function.execute(state.args.get(i), result, _collector);
            }
        }
    }
    
    private static class BatchState {
        public List<TridentTuple> tuples = new ArrayList<TridentTuple>();
        public List<TridentTuple> args = new ArrayList<TridentTuple>();
    }

    @Override
    public Factory getOutputFactory() {
        return _collector.getOutputFactory();
    } 
}
