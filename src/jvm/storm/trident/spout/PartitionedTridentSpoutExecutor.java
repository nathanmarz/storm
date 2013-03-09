package storm.trident.spout;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import storm.trident.operation.TridentCollector;
import storm.trident.topology.TransactionAttempt;
import storm.trident.topology.state.RotatingTransactionalState;
import storm.trident.topology.state.TransactionalState;


public class PartitionedTridentSpoutExecutor implements ITridentSpout<Integer> {
    IPartitionedTridentSpout _spout;
    
    public PartitionedTridentSpoutExecutor(IPartitionedTridentSpout spout) {
        _spout = spout;
    }
    
    public IPartitionedTridentSpout getPartitionedSpout() {
        return _spout;
    }
    
    class Coordinator implements ITridentSpout.BatchCoordinator<Object> {
        private IPartitionedTridentSpout.Coordinator _coordinator;
        
        public Coordinator(Map conf, TopologyContext context) {
            _coordinator = _spout.getCoordinator(conf, context);
        }
        
        @Override
        public Object initializeTransaction(long txid, Object prevMetadata, Object currMetadata) {
            if(currMetadata!=null) {
                return currMetadata;
            } else {
                return _coordinator.getPartitionsForBatch();            
            }
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
    
    class Emitter implements ITridentSpout.Emitter<Object> {
        private IPartitionedTridentSpout.Emitter _emitter;
        private TransactionalState _state;
        private Map<String, EmitterPartitionState> _partitionStates = new HashMap<String, EmitterPartitionState>();
        private int _index;
        private int _numTasks;
        
        public Emitter(String txStateId, Map conf, TopologyContext context) {
            _emitter = _spout.getEmitter(conf, context);
            _state = TransactionalState.newUserState(conf, txStateId); 
            _index = context.getThisTaskIndex();
            _numTasks = context.getComponentTasks(context.getThisComponentId()).size();
        }

        Object _savedCoordinatorMeta = null;

        
        @Override
        public void emitBatch(final TransactionAttempt tx, final Object coordinatorMeta,
                final TridentCollector collector) {
            if(_savedCoordinatorMeta == null || !_savedCoordinatorMeta.equals(coordinatorMeta)) {
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
            }
            for(EmitterPartitionState s: _partitionStates.values()) {
                RotatingTransactionalState state = s.rotatingState;
                final ISpoutPartition partition = s.partition;
                Object meta = state.getStateOrCreate(tx.getTransactionId(),
                        new RotatingTransactionalState.StateInitializer() {
                    @Override
                    public Object init(long txid, Object lastState) {
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
        public void success(TransactionAttempt tx) {
            for(EmitterPartitionState state: _partitionStates.values()) {
                state.rotatingState.cleanupBefore(tx.getTransactionId());
            }
        }

        @Override
        public void close() {
            _state.close();
            _emitter.close();
        }
    }    

    @Override
    public ITridentSpout.BatchCoordinator getCoordinator(String txStateId, Map conf, TopologyContext context) {
        return new Coordinator(conf, context);
    }

    @Override
    public ITridentSpout.Emitter getEmitter(String txStateId, Map conf, TopologyContext context) {
        return new Emitter(txStateId, conf, context);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return _spout.getComponentConfiguration();
    }

    @Override
    public Fields getOutputFields() {
        return _spout.getOutputFields();
    }    
}