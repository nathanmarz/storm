package backtype.storm.transactional;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IComponent;
import java.util.Map;

public interface ITransactionalSpout extends IComponent {
    ITransactionState getState();
    void open(Map conf, TopologyContext context);
    void close();
    // must always emit same batch for same transaction id
    // must emit attempt as first field in output tuple (any way to enforce this?)
    void emitBatch(TransactionAttempt tx, TransactionalOutputCollector collector);
    boolean isDistributed();
}
