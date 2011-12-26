package backtype.storm.transactional;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IComponent;
import java.util.Map;

public interface ITransactionalSpout extends IComponent {
    ICoordinatorState getState();
    void open(Map conf, TopologyContext context);
    void close();
    // must always emit same batch for same transaction id
    // must emit attempt as first field in output tuple (any way to enforce this?)
    void emitBatch(TransactionAttempt tx, TransactionalOutputCollector collector);
    // TODO: is there a way for this to automatically manage the getting, saving, and cleaning
    // of the batch paramaters for each transaction? how to deal with partitioning?
    // make a "partitionedtransactionalspout"? -- needs to be able to adjust partitions dynamically
    //  - partitions get evenly distributed among tasks automatically
    //  - partition for every batch request?
    //  - need to make sure adjusted partition doesn't screw up transaction retries (e.g., suddenly have data
    //  - from new partition)
    //  - maybe the coordinator needs to set this up and send it with the batch emit?
    //  - then the coordinator can manage cleanup too...
    boolean isDistributed();
}
