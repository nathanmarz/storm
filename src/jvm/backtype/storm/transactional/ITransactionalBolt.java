package backtype.storm.transactional;

import backtype.storm.task.BoltEmitter;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import java.io.Serializable;
import java.util.Map;

public interface ITransactionalBolt extends Serializable {
    void prepare(Map conf, TopologyContext context, String spoutId);
    void execute(Tuple tuple);
    // can anchor output tuples to the TransactionalTuple. commit on coordinator doesn't happen until
    // that tuple tree gets acked
    // need an "Anchorable" interface that can be used as anchors in output collectors...
    // just needs to expose get message id
    void commit(TransactionTuple transactionInfo, BoltEmitter collector);
}
