package backtype.storm.testing;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.transactional.TransactionalOutputCollector;
import backtype.storm.transactional.partitioned.IPartitionedTransactionalSpout;
import backtype.storm.transactional.partitioned.IPartitionedTransactionalSpout.Emitter;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.RegisteredGlobalState;
import backtype.storm.utils.Utils;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


// TODO:
//   - needs to be restartable
//   - needs to not advance past transactions it shouldn't be advancing to...
//   - don't make it dependent on the actual txid
// have it read form a source of partitions (passed in and registered)
// don't modify the source before calling complete-topology.
// total-tuples is just the number of tuples across all partitions
// to implement completed-tuples need to know the current offset for each partition in zookeeper...
//   - get the current txid from zk.
//   - get the start offset for each partition using that txid
//   - num tuples left is (size of partition queue - that offset)
public class MemoryTransactionalSpout implements IPartitionedTransactionalSpout<MemoryTransactionalSpoutMeta> {
    private String _id;
    private String _finishedPartitionsId;
    private int _takeAmt;
    private Fields _outFields;
    
    public MemoryTransactionalSpout(Map<Integer, List<List<Object>>> partitions, Fields outFields, int takeAmt) {
        _id = RegisteredGlobalState.registerState(partitions);
        Map<Integer, Boolean> finished = Collections.synchronizedMap(new HashMap<Integer, Boolean>());
        for(Integer partition: partitions.keySet()) {
            finished.put(partition, false);
        }
        _finishedPartitionsId = RegisteredGlobalState.registerState(finished);
        _takeAmt = takeAmt;
        _outFields = outFields;
    }
    
    public boolean isExhaustedTuples() {
        for(Integer partition: getQueues().keySet()) {
            if(!getFinishedStatuses().get(partition)) {
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
        public void close() {
        }        
    }
    
    class Emitter implements IPartitionedTransactionalSpout.Emitter<MemoryTransactionalSpoutMeta> {
        
        Integer _maxSpoutPending;
        Map<Integer, Integer> _emptyPartitions = new HashMap<Integer, Integer>();
        
        public Emitter(Map conf) {
            _maxSpoutPending = Utils.getInt(conf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING));
            if(_maxSpoutPending==null) _maxSpoutPending = 1;            
        }
        
        
        @Override
        public MemoryTransactionalSpoutMeta emitPartitionBatchNew(TransactionAttempt tx, TransactionalOutputCollector collector, int partition, MemoryTransactionalSpoutMeta lastPartitionMeta) {
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
            for(int i=index; i < index + toTake; i++) {
                collector.emit(queue.get(i));
            }
            if(toTake==0) {
                // this is a pretty hacky way to determine when all the partitions have been committed
                // wait until we've emitted max-spout-pending empty partitions for the partition
                int curr = Utils.get(_emptyPartitions, partition, 0) + 1;
                _emptyPartitions.put(partition, curr);
                if(curr > _maxSpoutPending) {
                    getFinishedStatuses().put(partition, true);
                }
            }
            return new MemoryTransactionalSpoutMeta(index, toTake);            
        }

        @Override
        public void emitPartitionBatch(TransactionAttempt tx, TransactionalOutputCollector collector, int partition, MemoryTransactionalSpoutMeta partitionMeta) {
            List<List<Object>> queue = getQueues().get(partition);
            for(int i=partitionMeta.index; i < partitionMeta.index + partitionMeta.amt; i++) {
                collector.emit(queue.get(i));                
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
        declarer.declare(_outFields);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.registerSerialization(MemoryTransactionalSpoutMeta.class);
        return conf;
    }
    
    public void cleanup() {
        RegisteredGlobalState.clearState(_id);
        RegisteredGlobalState.clearState(_finishedPartitionsId);
    }
    
    private Map<Integer, List<List<Object>>> getQueues() {
        return (Map<Integer, List<List<Object>>>) RegisteredGlobalState.getState(_id);
    }
    
    private Map<Integer, Boolean> getFinishedStatuses() {
        return (Map<Integer, Boolean>) RegisteredGlobalState.getState(_finishedPartitionsId);
    }
}
