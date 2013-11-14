package backtype.storm.transactional.partitioned;

import backtype.storm.Config;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.transactional.ICommitterTransactionalSpout;
import backtype.storm.transactional.ITransactionalSpout;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.transactional.state.RotatingTransactionalState;
import backtype.storm.transactional.state.TransactionalState;
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
        TreeMap<BigInteger, Map<Integer, Object>> _cachedMetas = new TreeMap<BigInteger, Map<Integer, Object>>();
        Map<Integer, RotatingTransactionalState> _partitionStates = new HashMap<Integer, RotatingTransactionalState>();
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
            Map<Integer, Object> metas = new HashMap<Integer, Object>();
            _cachedMetas.put(tx.getTransactionId(), metas);
            int partitions = _emitter.numPartitions();
            Entry<BigInteger, Map<Integer, Object>> entry = _cachedMetas.lowerEntry(tx.getTransactionId());
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
        public void cleanupBefore(BigInteger txid) {
            for(RotatingTransactionalState state: _partitionStates.values()) {
                state.cleanupBefore(txid);
            }            
        }

        @Override
        public void commit(TransactionAttempt attempt) {
            BigInteger txid = attempt.getTransactionId();
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
