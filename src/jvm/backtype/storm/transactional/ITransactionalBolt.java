package backtype.storm.transactional;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IComponent;
import backtype.storm.tuple.Tuple;
import java.io.Serializable;
import java.util.Map;

/**
 * Implement ICommittable to receive commits from the TransactionSpout
 */
public interface ITransactionalBolt extends Serializable, IComponent {
    void prepare(Map conf, TopologyContext context, TransactionalOutputCollector collector, TransactionAttempt attempt);
    void execute(Tuple tuple);
    void finishBatch();
}
