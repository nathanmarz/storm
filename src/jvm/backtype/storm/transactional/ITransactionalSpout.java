package backtype.storm.transactional;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IComponent;
import java.math.BigInteger;
import java.util.Map;

public interface ITransactionalSpout extends IComponent {
    public interface Coordinator {
        // this would be things like "# of partitions" when doing a kafka spout
        Object initializeTransaction(BigInteger txid, Object prevMetadata);
        void close();
    }
    
    public interface Emitter {
        // must always emit same batch for same transaction id
        // must emit attempt as first field in output tuple (any way to enforce this?)
        // for kafka: get up to X tuples, emit, store number of tuples for that partition in zk
        void emitBatch(TransactionAttempt tx, Object coordinatorMeta, TransactionalOutputCollector collector);
        //can do things like cleanup user state in zk
        void cleanupBefore(BigInteger txid);
        void close();
    }
    
    Coordinator getCoordinator(Map conf, TopologyContext context);
    Emitter getEmitter(Map conf, TopologyContext context);
}
