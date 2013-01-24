package storm.trident.spout;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import storm.trident.operation.TridentCollector;
import storm.trident.topology.TransactionAttempt;

/**
 * This interface defines a transactional spout that reads its tuples from a partitioned set of 
 * brokers. It automates the storing of metadata for each partition to ensure that the same batch
 * is always emitted for the same transaction id. The partition metadata is stored in Zookeeper.
 */
public interface IPartitionedTridentSpout<Partitions, Partition extends ISpoutPartition, T> extends Serializable {
    public interface Coordinator<Partitions> {
        /**
         * Return the partitions currently in the source of data. The idea is
         * is that if a new partition is added and a prior transaction is replayed, it doesn't
         * emit tuples for the new partition because it knows what partitions were in 
         * that transaction.
         */
        Partitions getPartitionsForBatch();
                
        boolean isReady(long txid);
        
        void close();
    }
    
    public interface Emitter<Partitions, Partition extends ISpoutPartition, X> {
        
        List<Partition> getOrderedPartitions(Partitions allPartitionInfo);
        
        /**
         * Emit a batch of tuples for a partition/transaction that's never been emitted before.
         * Return the metadata that can be used to reconstruct this partition/batch in the future.
         */
        X emitPartitionBatchNew(TransactionAttempt tx, TridentCollector collector, Partition partition, X lastPartitionMeta);

        /**
         * This method is called when this task is responsible for a new set of partitions. Should be used
         * to manage things like connections to brokers.
         */
        void refreshPartitions(List<Partition> partitionResponsibilities);
        
        /**
         * Emit a batch of tuples for a partition/transaction that has been emitted before, using
         * the metadata created when it was first emitted.
         */
        void emitPartitionBatch(TransactionAttempt tx, TridentCollector collector, Partition partition, X partitionMeta);
        void close();
    }
    
    Coordinator<Partitions> getCoordinator(Map conf, TopologyContext context);
    Emitter<Partitions, Partition, T> getEmitter(Map conf, TopologyContext context);
    
    Map getComponentConfiguration();
    Fields getOutputFields();
}
