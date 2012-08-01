package storm.trident.spout;


import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
        public Object initializeTransaction(long txid, Object prevMetadata) {
            return null;
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
    
    public class Emitter implements ICommitterTridentSpout.Emitter {
        IOpaquePartitionedTridentSpout.Emitter _emitter;
        TransactionalState _state;
        TreeMap<Long, Map<Integer, Object>> _cachedMetas = new TreeMap<Long, Map<Integer, Object>>();
        Map<Integer, RotatingTransactionalState> _partitionStates = new HashMap<Integer, RotatingTransactionalState>();
        int _index;
        int _numTasks;
        
        public Emitter(String txStateId, Map conf, TopologyContext context) {
            _emitter = _spout.getEmitter(conf, context);
            _index = context.getThisTaskIndex();
            _numTasks = context.getComponentTasks(context.getThisComponentId()).size();
            _state = TransactionalState.newUserState(conf, txStateId); 
            List<String> existingPartitions = _state.list("");
            for(String p: existingPartitions) {
                int partition = Integer.parseInt(p);
                if((partition - _index) % _numTasks == 0) {
                    _partitionStates.put(partition, new RotatingTransactionalState(_state, p));
                }
            }
        }
        
        @Override
        public void emitBatch(TransactionAttempt tx, Object coordinatorMeta, TridentCollector collector) {
            Map<Integer, Object> metas = new HashMap<Integer, Object>();
            _cachedMetas.put(tx.getTransactionId(), metas);
            long partitions = _emitter.numPartitions();
            Entry<Long, Map<Integer, Object>> entry = _cachedMetas.lowerEntry(tx.getTransactionId());
            Map<Integer, Object> prevCached;
            if(entry!=null) {
                prevCached = entry.getValue();
            } else {
                prevCached = new HashMap<Integer, Object>();
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
        public void success(TransactionAttempt tx) {
            for(RotatingTransactionalState state: _partitionStates.values()) {
                state.cleanupBefore(tx.getTransactionId());
            }            
        }

        @Override
        public void commit(TransactionAttempt attempt) {
            Long txid = attempt.getTransactionId();
            Map<Integer, Object> metas = _cachedMetas.remove(txid);
            for(Integer partition: metas.keySet()) {
                Object meta = metas.get(partition);
                _partitionStates.get(partition).overrideState(txid, meta);
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
