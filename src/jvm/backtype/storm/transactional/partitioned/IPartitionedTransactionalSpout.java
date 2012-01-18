package backtype.storm.transactional.partitioned;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IComponent;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.transactional.TransactionalOutputCollector;
import java.util.Map;

public interface IPartitionedTransactionalSpout extends IComponent {
    public interface Coordinator {
        int numPartitions();
        void close();
    }
    
    public interface Emitter {
        /**
         * Emit a batch of tuples for a partition/transaction that's never been emitted before.
         * Return the metadata that can be used to reconstruct this partition/batch in the future.
         */
        Object emitPartitionBatchNew(TransactionAttempt tx, TransactionalOutputCollector collector, int partition, Object lastPartitionMeta);
        /**
         * Emit a batch of tuples for a partition/transaction that's been emitted before, using
         * the metadata created when it was first emitted.
         */
        void emitPartitionBatch(TransactionAttempt tx, TransactionalOutputCollector collector, int partition, Object partitionMeta);
        void close();
    }
    
    Coordinator getCoordinator(Map conf, TopologyContext context);
    Emitter getEmitter(Map conf, TopologyContext context);      
}
