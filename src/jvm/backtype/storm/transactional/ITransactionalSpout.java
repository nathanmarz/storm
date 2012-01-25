package backtype.storm.transactional;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IComponent;
import java.math.BigInteger;
import java.util.Map;

public interface ITransactionalSpout<T> extends IComponent {
    public interface Coordinator<X> {
        // this would be things like "# of partitions" when doing a kafka spout
        X initializeTransaction(BigInteger txid, X prevMetadata);
        void close();
    }
    
    public interface Emitter<X> {
        // must always emit same batch for same transaction id
        // must emit attempt as first field in output tuple (any way to enforce this?)
        // for kafka: get up to X tuples, emit, store number of tuples for that partition in zk
        void emitBatch(TransactionAttempt tx, X coordinatorMeta, BatchOutputCollector collector);
        //can do things like cleanup user state in zk
        void cleanupBefore(BigInteger txid);
        void close();
    }
    
    Coordinator<T> getCoordinator(Map conf, TopologyContext context);
    Emitter<T> getEmitter(Map conf, TopologyContext context);
}
