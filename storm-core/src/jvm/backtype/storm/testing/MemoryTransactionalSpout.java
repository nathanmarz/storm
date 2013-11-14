package backtype.storm.testing;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.transactional.partitioned.IPartitionedTransactionalSpout;
import backtype.storm.transactional.partitioned.IPartitionedTransactionalSpout.Emitter;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.RegisteredGlobalState;
import backtype.storm.utils.Utils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MemoryTransactionalSpout implements IPartitionedTransactionalSpout<MemoryTransactionalSpoutMeta> {
    public static String TX_FIELD = MemoryTransactionalSpout.class.getName() + "/id";
    
    private String _id;
    private String _finishedPartitionsId;
    private int _takeAmt;
    private Fields _outFields;
    private Map<Integer, List<List<Object>>> _initialPartitions;
    
    public MemoryTransactionalSpout(Map<Integer, List<List<Object>>> partitions, Fields outFields, int takeAmt) {
        _id = RegisteredGlobalState.registerState(partitions);
        Map<Integer, Boolean> finished = Collections.synchronizedMap(new HashMap<Integer, Boolean>());
        _finishedPartitionsId = RegisteredGlobalState.registerState(finished);
        _takeAmt = takeAmt;
        _outFields = outFields;
        _initialPartitions = partitions;
    }
    
    public boolean isExhaustedTuples() {
        Map<Integer, Boolean> statuses = getFinishedStatuses();
        for(Integer partition: getQueues().keySet()) {
            if(!statuses.containsKey(partition) || !getFinishedStatuses().get(partition)) {
                return false;
            }
        }
        return true;
    }
    
    class Coordinator implements IPartitionedTransactionalSpout.Coordinator {

        @Override
        public int numPartitions() {
            return getQueues().size();
        }

        @Override
        public boolean isReady() {
            return true;
        }        
        
        @Override
        public void close() {
        }        
    }
    
    class Emitter implements IPartitionedTransactionalSpout.Emitter<MemoryTransactionalSpoutMeta> {
        
        Integer _maxSpoutPending;
        Map<Integer, Integer> _emptyPartitions = new HashMap<Integer, Integer>();
        
        public Emitter(Map conf) {
            Object c = conf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING);
            if(c==null) _maxSpoutPending = 1;
            else _maxSpoutPending = Utils.getInt(c);
        }
        
        
        @Override
        public MemoryTransactionalSpoutMeta emitPartitionBatchNew(TransactionAttempt tx, BatchOutputCollector collector, int partition, MemoryTransactionalSpoutMeta lastPartitionMeta) {
            int index;
            if(lastPartitionMeta==null) {
                index = 0;
            } else {
                index = lastPartitionMeta.index + lastPartitionMeta.amt;
            }
            List<List<Object>> queue = getQueues().get(partition);
            int total = queue.size();
            int left = total - index;
            int toTake = Math.min(left, _takeAmt);
            
            MemoryTransactionalSpoutMeta ret = new MemoryTransactionalSpoutMeta(index, toTake);
            emitPartitionBatch(tx, collector, partition, ret);
            if(toTake==0) {
                // this is a pretty hacky way to determine when all the partitions have been committed
                // wait until we've emitted max-spout-pending empty partitions for the partition
                int curr = Utils.get(_emptyPartitions, partition, 0) + 1;
                _emptyPartitions.put(partition, curr);
                if(curr > _maxSpoutPending) {
                    Map<Integer, Boolean> finishedStatuses = getFinishedStatuses();
                    // will be null in remote mode
                    if(finishedStatuses!=null) {
                        finishedStatuses.put(partition, true);
                    }
                }
            }
            return ret;   
        }

        @Override
        public void emitPartitionBatch(TransactionAttempt tx, BatchOutputCollector collector, int partition, MemoryTransactionalSpoutMeta partitionMeta) {
            List<List<Object>> queue = getQueues().get(partition);
            for(int i=partitionMeta.index; i < partitionMeta.index + partitionMeta.amt; i++) {
                List<Object> toEmit = new ArrayList<Object>(queue.get(i));
                toEmit.add(0, tx);
                collector.emit(toEmit);                
            }
        }
                
        @Override
        public void close() {
        }        
    } 
    
    @Override
    public IPartitionedTransactionalSpout.Coordinator getCoordinator(Map conf, TopologyContext context) {
        return new Coordinator();
    }

    @Override
    public IPartitionedTransactionalSpout.Emitter<MemoryTransactionalSpoutMeta> getEmitter(Map conf, TopologyContext context) {
        return new Emitter(conf);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        List<String> toDeclare = new ArrayList<String>(_outFields.toList());
        toDeclare.add(0, TX_FIELD);
        declarer.declare(new Fields(toDeclare));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.registerSerialization(MemoryTransactionalSpoutMeta.class);
        return conf;
    }
    
    public void startup() {
        getFinishedStatuses().clear();
    }
    
    public void cleanup() {
        RegisteredGlobalState.clearState(_id);
        RegisteredGlobalState.clearState(_finishedPartitionsId);
    }
    
    private Map<Integer, List<List<Object>>> getQueues() {   
        Map<Integer, List<List<Object>>> ret = (Map<Integer, List<List<Object>>>) RegisteredGlobalState.getState(_id);
        if(ret!=null) return ret;
        else return _initialPartitions;
    }
    
    private Map<Integer, Boolean> getFinishedStatuses() {
        return (Map<Integer, Boolean>) RegisteredGlobalState.getState(_finishedPartitionsId);
    }
}
