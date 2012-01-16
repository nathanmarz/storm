package backtype.storm.transactional;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IComponent;
import java.math.BigInteger;
import java.util.Map;

public interface ITransactionalSpout extends IComponent {
    public interface Coordinator {
        // this would be things like "# of partitions" when doing a kafka spout
        // can also do things like initialize user state in zk
        Object initialize(BigInteger txid, Object prevMetadata);
        void close();
    }
    
    public interface Emitter {
        // must always emit same batch for same transaction id
        // must emit attempt as first field in output tuple (any way to enforce this?)
        Object emitBatch(TransactionAttempt tx, TransactionalOutputCollector collector);
        //can do things like cleanup user state in zk
        void cleanupBefore(BigInteger txid);
        void close();
    }
    
    // this is used to store the state of this transactionalspout in zookeeper
    // it would be very dangerous to have 2 topologies active with the same id in the same cluster
    String getId();
    Coordinator getCoordinator(Map conf, TopologyContext context);
    Emitter getEmitter(Map conf, TopologyContext context);    
}
