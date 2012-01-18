package backtype.storm.transactional.partitioned;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.transactional.ITransactionalSpout;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.transactional.TransactionalOutputCollector;
import backtype.storm.transactional.state.RotatingTransactionalState;
import backtype.storm.transactional.state.TransactionalState;
import backtype.storm.utils.Utils;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;


public class PartitionedTransactionalSpoutExecutor implements ITransactionalSpout {
    IPartitionedTransactionalSpout _spout;
    
    public PartitionedTransactionalSpoutExecutor(IPartitionedTransactionalSpout spout) {
        _spout = spout;
    }
    
    class Coordinator implements ITransactionalSpout.Coordinator {
        private IPartitionedTransactionalSpout.Coordinator _coordinator;
        
        public Coordinator(Map conf, TopologyContext context) {
            _coordinator = _spout.getCoordinator(conf, context);
        }
        
        @Override
        public Object initializeTransaction(BigInteger txid, Object prevMetadata) {
            return _coordinator.numPartitions();
        }

        @Override
        public void close() {
            _coordinator.close();
        }        
    }
    
    class Emitter implements ITransactionalSpout.Emitter {
        private IPartitionedTransactionalSpout.Emitter _emitter;
        private TransactionalState _state;
        private Map<Integer, RotatingTransactionalState> _partitionStates = new HashMap<Integer, RotatingTransactionalState>();
        private int _index;
        private int _numTasks;
        
        public Emitter(Map conf, TopologyContext context) {
            _emitter = _spout.getEmitter(conf, context);
            _state = TransactionalState.newUserState(conf, (String) conf.get(Config.TOPOLOGY_TRANSACTIONAL_ID), PartitionedTransactionalSpoutExecutor.this); 
            _index = context.getThisTaskIndex();
            _numTasks = context.getComponentTasks(context.getThisComponentId()).size();
        }

        @Override
        public void emitBatch(final TransactionAttempt tx, final Object coordinatorMeta,
                final TransactionalOutputCollector collector) {
            int partitions = (Integer) coordinatorMeta;
            TreeMap<Integer, Integer> allocationsMap = Utils.integerDivided(partitions, _numTasks);
            List<Integer> allocations = new ArrayList<Integer>();
            for(Integer amt: allocationsMap.descendingKeySet()) {
                for(int i=0; i<allocationsMap.get(amt); i++) {
                    allocations.add(amt);
                }
            }
            int startPartition = 0;
            for(int i=0; i<_index; i++) {
                startPartition += allocations.get(i);
            }
            for(int i=startPartition; i < startPartition + allocations.get(_index); i+=1) {
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
