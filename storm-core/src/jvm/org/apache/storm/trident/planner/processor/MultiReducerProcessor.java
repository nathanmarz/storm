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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.trident.operation.MultiReducer;
import org.apache.storm.trident.operation.TridentMultiReducerContext;
import org.apache.storm.trident.planner.ProcessorContext;
import org.apache.storm.trident.planner.TridentProcessor;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.tuple.TridentTuple.Factory;
import org.apache.storm.trident.tuple.TridentTupleView.ProjectionFactory;


public class MultiReducerProcessor implements TridentProcessor {
    MultiReducer _reducer;
    TridentContext _context;
    Map<String, Integer> _streamToIndex;
    List<Fields> _projectFields;
    ProjectionFactory[] _projectionFactories;
    FreshCollector _collector;

    public MultiReducerProcessor(List<Fields> inputFields, MultiReducer reducer) {
        _reducer = reducer;
        _projectFields = inputFields;
    }
    
    @Override
    public void prepare(Map conf, TopologyContext context, TridentContext tridentContext) {
        List<Factory> parents = tridentContext.getParentTupleFactories();
        _context = tridentContext;
        _streamToIndex = new HashMap<>();
        List<String> parentStreams = tridentContext.getParentStreams();
        for(int i=0; i<parentStreams.size(); i++) {
            _streamToIndex.put(parentStreams.get(i), i);
        }
        _projectionFactories = new ProjectionFactory[_projectFields.size()];
        for(int i=0; i<_projectFields.size(); i++) {
            _projectionFactories[i] = new ProjectionFactory(parents.get(i), _projectFields.get(i));
        }
        _collector = new FreshCollector(tridentContext);
        _reducer.prepare(conf, new TridentMultiReducerContext((List) Arrays.asList(_projectionFactories)));
    }

    @Override
    public void cleanup() {
        _reducer.cleanup();
    }

    @Override
    public void startBatch(ProcessorContext processorContext) {
        _collector.setContext(processorContext);
        processorContext.state[_context.getStateIndex()] = _reducer.init(_collector);
    }    

    @Override
    public void execute(ProcessorContext processorContext, String streamId, TridentTuple tuple) {
        _collector.setContext(processorContext);
        int i = _streamToIndex.get(streamId);
        _reducer.execute(processorContext.state[_context.getStateIndex()], i, _projectionFactories[i].create(tuple), _collector);
    }
    
    @Override
    public void finishBatch(ProcessorContext processorContext) {
        _collector.setContext(processorContext);
        _reducer.complete(processorContext.state[_context.getStateIndex()], _collector);
    }
 
    @Override
    public Factory getOutputFactory() {
        return _collector.getOutputFactory();
    } 
}
