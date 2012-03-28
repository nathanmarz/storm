package backtype.storm.transactional.partitioned;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IComponent;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.coordination.BatchOutputCollector;
import java.util.Map;

/**
 * This interface defines a transactional spout that reads its tuples from a partitioned set of 
 * brokers. It automates the storing of metadata for each partition to ensure that the same batch
 * is always emitted for the same transaction id. The partition metadata is stored in Zookeeper.
 */
public interface IPartitionedTransactionalSpout<T> extends IComponent {
    public interface Coordinator {
        /**
         * Return the number of partitions currently in the source of data. The idea is
         * is that if a new partition is added and a prior transaction is replayed, it doesn't
         * emit tuples for the new partition because it knows how many partitions were in 
         * that transaction.
         */
        int numPartitions();
        
        /**
         * Returns true if its ok to emit start a new transaction, false otherwise (will skip this transaction).
         * 
         * You should sleep here if you want a delay between asking for the next transaction (this will be called 
         * repeatedly in a loop).
         */
        boolean isReady();
                
        void close();
    }
    
    public interface Emitter<X> {
        /**
         * Emit a batch of tuples for a partition/transaction that's never been emitted before.
         * Return the metadata that can be used to reconstruct this partition/batch in the future.
         */
        X emitPartitionBatchNew(TransactionAttempt tx, BatchOutputCollector collector, int partition, X lastPartitionMeta);

        /**
         * Emit a batch of tuples for a partition/transaction that has been emitted before, using
         * the metadata created when it was first emitted.
         */
        void emitPartitionBatch(TransactionAttempt tx, BatchOutputCollector collector, int partition, X partitionMeta);
        void close();
    }
    
    Coordinator getCoordinator(Map conf, TopologyContext context);
    Emitter<T> getEmitter(Map conf, TopologyContext context);      
}
