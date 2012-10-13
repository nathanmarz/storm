package storm.trident.spout;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import storm.trident.operation.TridentCollector;
import storm.trident.topology.TransactionAttempt;

/**
 * This defines a transactional spout which does *not* necessarily
 * replay the same batch every time it emits a batch for a transaction id.
 * 
 */
public interface IOpaquePartitionedTridentSpout<Partitions, Partition extends ISpoutPartition, M> extends Serializable {
    public interface Coordinator<Partitions> {
        boolean isReady(long txid);
        Partitions getPartitionsForBatch();
        void close();
    }
    
    public interface Emitter<Partitions, Partition extends ISpoutPartition, M> {
        /**
         * Emit a batch of tuples for a partition/transaction. 
         * 
         * Return the metadata describing this batch that will be used as lastPartitionMeta
         * for defining the parameters of the next batch.
         */
        M emitPartitionBatch(TransactionAttempt tx, TridentCollector collector, Partition partition, M lastPartitionMeta);
        
        /**
         * This method is called when this task is responsible for a new set of partitions. Should be used
         * to manage things like connections to brokers.
         */        
        void refreshPartitions(List<Partition> partitionResponsibilities);        
        List<Partition> getOrderedPartitions(Partitions allPartitionInfo);
        void close();
    }
    
    Emitter<Partitions, Partition, M> getEmitter(Map conf, TopologyContext context);     
    Coordinator getCoordinator(Map conf, TopologyContext context);     
    Map getComponentConfiguration();
    Fields getOutputFields();
}