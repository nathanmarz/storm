package backtype.storm.transactional;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IComponent;
import backtype.storm.tuple.Tuple;
import java.util.Map;

public interface ICommitterBolt extends IComponent {
    void prepare(Map conf, TopologyContext context, TransactionAttempt attempt);
    void execute(Tuple tuple);
    void commit(BatchOutputCollector collector);
}
