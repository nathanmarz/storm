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
package org.apache.storm.transactional.partitioned;

import org.apache.storm.Config;
import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.transactional.ICommitterTransactionalSpout;
import org.apache.storm.transactional.ITransactionalSpout;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.transactional.state.RotatingTransactionalState;
import org.apache.storm.transactional.state.TransactionalState;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;


public class OpaquePartitionedTransactionalSpoutExecutor implements ICommitterTransactionalSpout<Object> {
    IOpaquePartitionedTransactionalSpout _spout;
    
    public class Coordinator implements ITransactionalSpout.Coordinator<Object> {
        IOpaquePartitionedTransactionalSpout.Coordinator _coordinator;

        public Coordinator(Map conf, TopologyContext context) {
            _coordinator = _spout.getCoordinator(conf, context);
        }
        
        @Override
        public Object initializeTransaction(BigInteger txid, Object prevMetadata) {
            return null;
        }

        @Override
        public boolean isReady() {
            return _coordinator.isReady();
        }        

        @Override
        public void close() {
            _coordinator.close();
        }        
    }
    
    public class Emitter implements ICommitterTransactionalSpout.Emitter {
        IOpaquePartitionedTransactionalSpout.Emitter _emitter;
        TransactionalState _state;
        TreeMap<BigInteger, Map<Integer, Object>> _cachedMetas = new TreeMap<>();
        Map<Integer, RotatingTransactionalState> _partitionStates = new HashMap<>();
        int _index;
        int _numTasks;
        
        public Emitter(Map conf, TopologyContext context) {
            _emitter = _spout.getEmitter(conf, context);
            _index = context.getThisTaskIndex();
            _numTasks = context.getComponentTasks(context.getThisComponentId()).size();
            _state = TransactionalState.newUserState(conf, (String) conf.get(Config.TOPOLOGY_TRANSACTIONAL_ID), getComponentConfiguration()); 
            List<String> existingPartitions = _state.list("");
            for(String p: existingPartitions) {
                int partition = Integer.parseInt(p);
                if((partition - _index) % _numTasks == 0) {
                    _partitionStates.put(partition, new RotatingTransactionalState(_state, p));
                }
            }
        }
        
        @Override
        public void emitBatch(TransactionAttempt tx, Object coordinatorMeta, BatchOutputCollector collector) {
            Map<Integer, Object> metas = new HashMap<>();
            _cachedMetas.put(tx.getTransactionId(), metas);
            int partitions = _emitter.numPartitions();
            Entry<BigInteger, Map<Integer, Object>> entry = _cachedMetas.lowerEntry(tx.getTransactionId());
            Map<Integer, Object> prevCached;
            if(entry!=null) {
                prevCached = entry.getValue();
            } else {
                prevCached = new HashMap<>();
            }
            
            for(int i=_index; i < partitions; i+=_numTasks) {
                RotatingTransactionalState state = _partitionStates.get(i);
                if(state==null) {
                    state = new RotatingTransactionalState(_state, "" + i);
                    _partitionStates.put(i, state);
                }
                state.removeState(tx.getTransactionId());
                Object lastMeta = prevCached.get(i);
                if(lastMeta==null) lastMeta = state.getLastState();
                Object meta = _emitter.emitPartitionBatch(tx, collector, i, lastMeta);
                metas.put(i, meta);
            }
        }

        @Override
        public void cleanupBefore(BigInteger txid) {
            for(RotatingTransactionalState state: _partitionStates.values()) {
                state.cleanupBefore(txid);
            }            
        }

        @Override
        public void commit(TransactionAttempt attempt) {
            BigInteger txid = attempt.getTransactionId();
            Map<Integer, Object> metas = _cachedMetas.remove(txid);
            for(Entry<Integer, Object> entry: metas.entrySet()) {
                Integer partition = entry.getKey();
                Object meta = entry.getValue();
                _partitionStates.get(partition).overrideState(txid, meta);
            }
        }

        @Override
        public void close() {
            _emitter.close();
        }
    } 
    
    public OpaquePartitionedTransactionalSpoutExecutor(IOpaquePartitionedTransactionalSpout spout) {
        _spout = spout;
    }
    
    @Override
    public ITransactionalSpout.Coordinator<Object> getCoordinator(Map conf, TopologyContext context) {
        return new Coordinator(conf, context);
    }

    @Override
    public ICommitterTransactionalSpout.Emitter getEmitter(Map conf, TopologyContext context) {
        return new Emitter(conf, context);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        _spout.declareOutputFields(declarer);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return _spout.getComponentConfiguration();
    }
    
}
