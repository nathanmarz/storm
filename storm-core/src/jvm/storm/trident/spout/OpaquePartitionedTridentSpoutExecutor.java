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
package storm.trident.spout;


import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import storm.trident.operation.TridentCollector;
import storm.trident.topology.state.RotatingTransactionalState;
import storm.trident.topology.state.TransactionalState;
import storm.trident.topology.TransactionAttempt;


public class OpaquePartitionedTridentSpoutExecutor implements ICommitterTridentSpout<Object> {
    IOpaquePartitionedTridentSpout _spout;
    
    public class Coordinator implements ITridentSpout.BatchCoordinator<Object> {
        IOpaquePartitionedTridentSpout.Coordinator _coordinator;

        public Coordinator(Map conf, TopologyContext context) {
            _coordinator = _spout.getCoordinator(conf, context);
        }
        
        @Override
        public Object initializeTransaction(long txid, Object prevMetadata, Object currMetadata) {
            return _coordinator.getPartitionsForBatch();
        }

        @Override
        public void close() {
            _coordinator.close();
        }

        @Override
        public void success(long txid) {
        }

        @Override
        public boolean isReady(long txid) {
            return _coordinator.isReady(txid);
        }
    }
    
    static class EmitterPartitionState {
        public RotatingTransactionalState rotatingState;
        public ISpoutPartition partition;
        
        public EmitterPartitionState(RotatingTransactionalState s, ISpoutPartition p) {
            rotatingState = s;
            partition = p;
        }
    }
    
    public class Emitter implements ICommitterTridentSpout.Emitter {        
        IOpaquePartitionedTridentSpout.Emitter _emitter;
        TransactionalState _state;
        TreeMap<Long, Map<String, Object>> _cachedMetas = new TreeMap<Long, Map<String, Object>>();
        Map<String, EmitterPartitionState> _partitionStates = new HashMap<String, EmitterPartitionState>();
        int _index;
        int _numTasks;
        
        public Emitter(String txStateId, Map conf, TopologyContext context) {
            _emitter = _spout.getEmitter(conf, context);
            _index = context.getThisTaskIndex();
            _numTasks = context.getComponentTasks(context.getThisComponentId()).size();
            _state = TransactionalState.newUserState(conf, txStateId);             
        }
        
        Object _savedCoordinatorMeta = null;
        boolean _changedMeta = false;
        
        @Override
        public void emitBatch(TransactionAttempt tx, Object coordinatorMeta, TridentCollector collector) {
            if(_savedCoordinatorMeta==null || !_savedCoordinatorMeta.equals(coordinatorMeta)) {
                List<ISpoutPartition> partitions = _emitter.getOrderedPartitions(coordinatorMeta);
                _partitionStates.clear();
                List<ISpoutPartition> myPartitions = new ArrayList();
                for(int i=_index; i < partitions.size(); i+=_numTasks) {
                    ISpoutPartition p = partitions.get(i);
                    String id = p.getId();
                    myPartitions.add(p);
                    _partitionStates.put(id, new EmitterPartitionState(new RotatingTransactionalState(_state, id), p));
                }
                _emitter.refreshPartitions(myPartitions);
                _savedCoordinatorMeta = coordinatorMeta;
                _changedMeta = true;
            }
            Map<String, Object> metas = new HashMap<String, Object>();
            _cachedMetas.put(tx.getTransactionId(), metas);

            Entry<Long, Map<String, Object>> entry = _cachedMetas.lowerEntry(tx.getTransactionId());
            Map<String, Object> prevCached;
            if(entry!=null) {
                prevCached = entry.getValue();
            } else {
                prevCached = new HashMap<String, Object>();
            }
            
            for(String id: _partitionStates.keySet()) {
                EmitterPartitionState s = _partitionStates.get(id);
                s.rotatingState.removeState(tx.getTransactionId());
                Object lastMeta = prevCached.get(id);
                if(lastMeta==null) lastMeta = s.rotatingState.getLastState();
                Object meta = _emitter.emitPartitionBatch(tx, collector, s.partition, lastMeta);
                metas.put(id, meta);
            }
        }

        @Override
        public void success(TransactionAttempt tx) {
            for(EmitterPartitionState state: _partitionStates.values()) {
                state.rotatingState.cleanupBefore(tx.getTransactionId());
            }            
        }

        @Override
        public void commit(TransactionAttempt attempt) {
            // this code here handles a case where a previous commit failed, and the partitions
            // changed since the last commit. This clears out any state for the removed partitions
            // for this txid.
            // we make sure only a single task ever does this. we're also guaranteed that
            // it's impossible for there to be another writer to the directory for that partition
            // because only a single commit can be happening at once. this is because in order for 
            // another attempt of the batch to commit, the batch phase must have succeeded in between.
            // hence, all tasks for the prior commit must have finished committing (whether successfully or not)
            if(_changedMeta && _index==0) {
                Set<String> validIds = new HashSet<String>();
                for(ISpoutPartition p: (List<ISpoutPartition>) _emitter.getOrderedPartitions(_savedCoordinatorMeta)) {
                    validIds.add(p.getId());
                }
                for(String existingPartition: _state.list("")) {
                    if(!validIds.contains(existingPartition)) {
                        RotatingTransactionalState s = new RotatingTransactionalState(_state, existingPartition);
                        s.removeState(attempt.getTransactionId());
                    }
                }
                _changedMeta = false;
            }            
            
            Long txid = attempt.getTransactionId();
            Map<String, Object> metas = _cachedMetas.remove(txid);
            for(String partitionId: metas.keySet()) {
                Object meta = metas.get(partitionId);
                _partitionStates.get(partitionId).rotatingState.overrideState(txid, meta);
            }
        }

        @Override
        public void close() {
            _emitter.close();
        }
    } 
    
    public OpaquePartitionedTridentSpoutExecutor(IOpaquePartitionedTridentSpout spout) {
        _spout = spout;
    }
    
    @Override
    public ITridentSpout.BatchCoordinator<Object> getCoordinator(String txStateId, Map conf, TopologyContext context) {
        return new Coordinator(conf, context);
    }

    @Override
    public ICommitterTridentSpout.Emitter getEmitter(String txStateId, Map conf, TopologyContext context) {
        return new Emitter(txStateId, conf, context);
    }

    @Override
    public Fields getOutputFields() {
        return _spout.getOutputFields();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return _spout.getComponentConfiguration();
    }
    
}
