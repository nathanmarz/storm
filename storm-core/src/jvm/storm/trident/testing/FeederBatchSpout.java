package storm.trident.testing;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.RegisteredGlobalState;
import backtype.storm.utils.Utils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout;
import storm.trident.topology.TransactionAttempt;
import storm.trident.topology.TridentTopologyBuilder;

public class FeederBatchSpout implements ITridentSpout, IFeeder {

    String _id;
    String _semaphoreId;
    Fields _outFields;
    boolean _waitToEmit = true;
    

    public FeederBatchSpout(List<String> fields) {
        _outFields = new Fields(fields);
        _id = RegisteredGlobalState.registerState(new CopyOnWriteArrayList());
        _semaphoreId = RegisteredGlobalState.registerState(new CopyOnWriteArrayList());
    }
    
    public void setWaitToEmit(boolean trueIfWait) {
        _waitToEmit = trueIfWait;
    }
    
    public void feed(Object tuples) {
        Semaphore sem = new Semaphore(0);
        ((List)RegisteredGlobalState.getState(_semaphoreId)).add(sem);
        ((List)RegisteredGlobalState.getState(_id)).add(tuples);
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    

    public class FeederCoordinator implements ITridentSpout.BatchCoordinator<Map<Integer, List<List<Object>>>> {

        int _numPartitions;
        int _emittedIndex = 0;
        Map<Long, Integer> txIndices = new HashMap();
        
        public FeederCoordinator(int numPartitions) {
            _numPartitions = numPartitions;
        }
        
        @Override
        public Map<Integer, List<List<Object>>> initializeTransaction(long txid, Map<Integer, List<List<Object>>> prevMetadata, Map<Integer, List<List<Object>>> currMetadata) {
            if(currMetadata!=null) return currMetadata;
            List allBatches = (List) RegisteredGlobalState.getState(_id);
            if(allBatches.size()>_emittedIndex) {
                Object batchInfo = allBatches.get(_emittedIndex);                
                txIndices.put(txid, _emittedIndex);                
                _emittedIndex += 1;
                if(batchInfo instanceof Map) {
                    return (Map) batchInfo;
                } else {
                    List batchList = (List) batchInfo;
                    Map<Integer, List<List<Object>>> partitions = new HashMap();
                    for(int i=0; i<_numPartitions; i++) {
                        partitions.put(i, new ArrayList());
                    }
                    for(int i=0; i<batchList.size(); i++) {
                        int partition = i % _numPartitions;
                        partitions.get(partition).add((List)batchList.get(i));
                    }
                    return partitions;
                }
            } else {
                return new HashMap();
            }
        }

        @Override
        public void close() {
        }

        @Override
        public void success(long txid) {
            Integer index = txIndices.get(txid);
            if(index != null) {
                Semaphore sem = (Semaphore) ((List)RegisteredGlobalState.getState(_semaphoreId)).get(index);
                sem.release();
            }
        }

        int _masterEmitted = 0;
        
        @Override
        public boolean isReady(long txid) {
            if(!_waitToEmit) return true;
            List allBatches = (List) RegisteredGlobalState.getState(_id);
            if(allBatches.size() > _masterEmitted) {
                _masterEmitted++;
                return true;
            } else {
                Utils.sleep(2);
                return false;
            }
        }
    }
    
    public class FeederEmitter implements ITridentSpout.Emitter<Map<Integer, List<List<Object>>>> {

        int _index;
        
        public FeederEmitter(int index) {
            _index = index;
        }
        
        @Override
        public void emitBatch(TransactionAttempt tx, Map<Integer, List<List<Object>>> coordinatorMeta, TridentCollector collector) {
            List<List<Object>> tuples = coordinatorMeta.get(_index);
            if(tuples!=null) {
                for(List<Object> t: tuples) {
                    collector.emit(t);
                }
            }
        }

        @Override
        public void success(TransactionAttempt tx) {
        }

        @Override
        public void close() {
        }        
    }
    
    
    @Override
    public Map getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return _outFields;
    }

    @Override
    public BatchCoordinator getCoordinator(String txStateId, Map conf, TopologyContext context) {
        int numTasks = context.getComponentTasks(
                            TridentTopologyBuilder.spoutIdFromCoordinatorId(
                                context.getThisComponentId()))
                                    .size();
        return new FeederCoordinator(numTasks);
    }

    @Override
    public Emitter getEmitter(String txStateId, Map conf, TopologyContext context) {
        return new FeederEmitter(context.getThisTaskIndex());
    }
    
    
    
}
