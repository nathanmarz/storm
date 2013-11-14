package backtype.storm.transactional.partitioned;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IComponent;
import backtype.storm.transactional.TransactionAttempt;
import java.util.Map;

/**
 * This defines a transactional spout which does *not* necessarily
 * replay the same batch every time it emits a batch for a transaction id.
 */
public interface IOpaquePartitionedTransactionalSpout<T> extends IComponent {
    public interface Coordinator {
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
         * Emit a batch of tuples for a partition/transaction. 
         * 
         * Return the metadata describing this batch that will be used as lastPartitionMeta
         * for defining the parameters of the next batch.
         */
        X emitPartitionBatch(TransactionAttempt tx, BatchOutputCollector collector, int partition, X lastPartitionMeta);
        int numPartitions();
        void close();
    }
    
    Emitter<T> getEmitter(Map conf, TopologyContext context);     
    Coordinator getCoordinator(Map conf, TopologyContext context);     
}
