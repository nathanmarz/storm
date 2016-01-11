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
import org.apache.storm.trident.topology.TransactionAttempt;
import org.apache.storm.tuple.Fields;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.planner.ProcessorContext;
import org.apache.storm.trident.planner.TridentProcessor;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.tuple.TridentTuple.Factory;
import org.apache.storm.trident.tuple.TridentTupleView.ProjectionFactory;


public class PartitionPersistProcessor implements TridentProcessor {
    StateUpdater _updater;
    State _state;
    String _stateId;
    TridentContext _context;
    Fields _inputFields;
    ProjectionFactory _projection;
    FreshCollector _collector;

    public PartitionPersistProcessor(String stateId, Fields inputFields, StateUpdater updater) {
        _updater = updater;
        _stateId = stateId;
        _inputFields = inputFields;
    }
    
    @Override
    public void prepare(Map conf, TopologyContext context, TridentContext tridentContext) {
        List<Factory> parents = tridentContext.getParentTupleFactories();
        if(parents.size()!=1) {
            throw new RuntimeException("Partition persist operation can only have one parent");
        }
        _context = tridentContext;
        _state = (State) context.getTaskData(_stateId);
        _projection = new ProjectionFactory(parents.get(0), _inputFields);
        _collector = new FreshCollector(tridentContext);
        _updater.prepare(conf, new TridentOperationContext(context, _projection));
    }

    @Override
    public void cleanup() {
        _updater.cleanup();
    }

    @Override
    public void startBatch(ProcessorContext processorContext) {
        processorContext.state[_context.getStateIndex()] = new ArrayList<TridentTuple>();        
    }
    
    @Override
    public void execute(ProcessorContext processorContext, String streamId, TridentTuple tuple) {
        ((List) processorContext.state[_context.getStateIndex()]).add(_projection.create(tuple));
    }

    @Override
    public void finishBatch(ProcessorContext processorContext) {
        _collector.setContext(processorContext);
        Object batchId = processorContext.batchId;
        // since this processor type is a committer, this occurs in the commit phase
        List<TridentTuple> buffer = (List) processorContext.state[_context.getStateIndex()];
        
        // don't update unless there are tuples
        // this helps out with things like global partition persist, where multiple tasks may still
        // exist for this processor. Only want the global one to do anything
        // this is also a helpful optimization that state implementations don't need to manually do
        if(buffer.size() > 0) {
            Long txid = null;
            // this is to support things like persisting off of drpc stream, which is inherently unreliable
            // and won't have a tx attempt
            if(batchId instanceof TransactionAttempt) {
                txid = ((TransactionAttempt) batchId).getTransactionId();
            }
            _state.beginCommit(txid);
            _updater.updateState(_state, buffer, _collector);
            _state.commit(txid);
        }
    }    

    @Override
    public Factory getOutputFactory() {
        return _collector.getOutputFactory();
    } 
}
