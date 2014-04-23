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
package backtype.storm.transactional.partitioned;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.transactional.ITransactionalSpout;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.transactional.state.RotatingTransactionalState;
import backtype.storm.transactional.state.TransactionalState;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;


public class PartitionedTransactionalSpoutExecutor implements ITransactionalSpout<Integer> {
    IPartitionedTransactionalSpout _spout;
    
    public PartitionedTransactionalSpoutExecutor(IPartitionedTransactionalSpout spout) {
        _spout = spout;
    }
    
    public IPartitionedTransactionalSpout getPartitionedSpout() {
        return _spout;
    }
    
    class Coordinator implements ITransactionalSpout.Coordinator<Integer> {
        private IPartitionedTransactionalSpout.Coordinator _coordinator;
        
        public Coordinator(Map conf, TopologyContext context) {
            _coordinator = _spout.getCoordinator(conf, context);
        }
        
        @Override
        public Integer initializeTransaction(BigInteger txid, Integer prevMetadata) {
            return _coordinator.numPartitions();
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
    
    class Emitter implements ITransactionalSpout.Emitter<Integer> {
        private IPartitionedTransactionalSpout.Emitter _emitter;
        private TransactionalState _state;
        private Map<Integer, RotatingTransactionalState> _partitionStates = new HashMap<Integer, RotatingTransactionalState>();
        private int _index;
        private int _numTasks;
        
        public Emitter(Map conf, TopologyContext context) {
            _emitter = _spout.getEmitter(conf, context);
            _state = TransactionalState.newUserState(conf, (String) conf.get(Config.TOPOLOGY_TRANSACTIONAL_ID), getComponentConfiguration()); 
            _index = context.getThisTaskIndex();
            _numTasks = context.getComponentTasks(context.getThisComponentId()).size();
        }

        @Override
        public void emitBatch(final TransactionAttempt tx, final Integer partitions,
                final BatchOutputCollector collector) {
            for(int i=_index; i < partitions; i+=_numTasks) {
                if(!_partitionStates.containsKey(i)) {
                    _partitionStates.put(i, new RotatingTransactionalState(_state, "" + i));
                }
                RotatingTransactionalState state = _partitionStates.get(i);
                final int partition = i;
                Object meta = state.getStateOrCreate(tx.getTransactionId(),
                        new RotatingTransactionalState.StateInitializer() {
                    @Override
                    public Object init(BigInteger txid, Object lastState) {
                        return _emitter.emitPartitionBatchNew(tx, collector, partition, lastState);
                    }
                });
                // it's null if one of:
                //   a) a later transaction batch was emitted before this, so we should skip this batch
                //   b) if didn't exist and was created (in which case the StateInitializer was invoked and 
                //      it was emitted
                if(meta!=null) {
                    _emitter.emitPartitionBatch(tx, collector, partition, meta);
                }
            }
            
        }

        @Override
        public void cleanupBefore(BigInteger txid) {
            for(RotatingTransactionalState state: _partitionStates.values()) {
                state.cleanupBefore(txid);
            }
        }

        @Override
        public void close() {
            _state.close();
            _emitter.close();
        }
    }    

    @Override
    public ITransactionalSpout.Coordinator getCoordinator(Map conf, TopologyContext context) {
        return new Coordinator(conf, context);
    }

    @Override
    public ITransactionalSpout.Emitter getEmitter(Map conf, TopologyContext context) {
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
