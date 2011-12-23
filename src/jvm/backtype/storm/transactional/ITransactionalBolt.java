package backtype.storm.transactional;

import backtype.storm.task.BoltEmitter;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import java.io.Serializable;
import java.util.Map;

public interface ITransactionalBolt extends Serializable {
    void prepare(Map conf, TopologyContext context, int txid);
    void execute(Tuple tuple);
    void commit(TransactionTuple transactionInfo, BoltEmitter collector);
}
