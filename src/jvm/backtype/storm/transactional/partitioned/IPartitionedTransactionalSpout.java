package backtype.storm.transactional.partitioned;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IComponent;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.coordination.BatchOutputCollector;
import java.util.Map;

public interface IPartitionedTransactionalSpout<T> extends IComponent {
    public interface Coordinator {
        int numPartitions();
        void close();
    }
    
    public interface Emitter<X> {
        /**
         * Emit a batch of tuples for a partition/transaction that's never been emitted before.
         * Return the metadata that can be used to reconstruct this partition/batch in the future.
         */
        X emitPartitionBatchNew(TransactionAttempt tx, BatchOutputCollector collector, int partition, X lastPartitionMeta);
        /**
         * Emit a batch of tuples for a partition/transaction that's been emitted before, using
         * the metadata created when it was first emitted.
         */
        void emitPartitionBatch(TransactionAttempt tx, BatchOutputCollector collector, int partition, X partitionMeta);
        void close();
    }
    
    Coordinator getCoordinator(Map conf, TopologyContext context);
    Emitter<T> getEmitter(Map conf, TopologyContext context);      
}
