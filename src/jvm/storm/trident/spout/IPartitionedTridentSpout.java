package storm.trident.spout;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import java.io.Serializable;
import java.util.Map;
import storm.trident.operation.TridentCollector;
import storm.trident.topology.TransactionAttempt;

/**
 * This interface defines a transactional spout that reads its tuples from a partitioned set of 
 * brokers. It automates the storing of metadata for each partition to ensure that the same batch
 * is always emitted for the same transaction id. The partition metadata is stored in Zookeeper.
 */
public interface IPartitionedTridentSpout<T> extends Serializable {
    public interface Coordinator {
        /**
         * Return the number of partitions currently in the source of data. The idea is
         * is that if a new partition is added and a prior transaction is replayed, it doesn't
         * emit tuples for the new partition because it knows how many partitions were in 
         * that transaction.
         */
        long numPartitions();
                
        boolean isReady(long txid);
        
        void close();
    }
    
    public interface Emitter<X> {
        /**
         * Emit a batch of tuples for a partition/transaction that's never been emitted before.
         * Return the metadata that can be used to reconstruct this partition/batch in the future.
         */
        X emitPartitionBatchNew(TransactionAttempt tx, TridentCollector collector, int partition, X lastPartitionMeta);

        /**
         * Emit a batch of tuples for a partition/transaction that has been emitted before, using
         * the metadata created when it was first emitted.
         */
        void emitPartitionBatch(TransactionAttempt tx, TridentCollector collector, int partition, X partitionMeta);
        void close();
    }
    
    Coordinator getCoordinator(Map conf, TopologyContext context);
    Emitter<T> getEmitter(Map conf, TopologyContext context);
    
    Map getComponentConfiguration();
    Fields getOutputFields();
}
