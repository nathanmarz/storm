package storm.trident.spout;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import java.io.Serializable;
import java.util.Map;
import storm.trident.operation.TridentCollector;
import storm.trident.topology.TransactionAttempt;

/**
 * This defines a transactional spout which does *not* necessarily
 * replay the same batch every time it emits a batch for a transaction id.
 */
public interface IOpaquePartitionedTridentSpout<T> extends Serializable {
    public interface Coordinator {
        boolean isReady(long txid);
        void close();
    }
    
    public interface Emitter<X> {
        /**
         * Emit a batch of tuples for a partition/transaction. 
         * 
         * Return the metadata describing this batch that will be used as lastPartitionMeta
         * for defining the parameters of the next batch.
         */
        X emitPartitionBatch(TransactionAttempt tx, TridentCollector collector, int partition, X lastPartitionMeta);
        long numPartitions();
        void close();
    }
    
    Emitter<T> getEmitter(Map conf, TopologyContext context);     
    Coordinator getCoordinator(Map conf, TopologyContext context);     
    Map getComponentConfiguration();
    Fields getOutputFields();
}