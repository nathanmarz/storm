package backtype.storm.transactional;

import java.util.Map;

// TODO: acts identically to a bolt EXCEPT for the coordinator task which:
//  -- keeps track of open transactions
//  -- persists the state of committed transactions
//  -- sends out batch and commit tuples
public interface ITransactionalSpout {
    void open(Map conf);
    void close();
    void setTransactionId(long txid);
    long getTransactionId();
    void emitBatch(TransactionAttempt tx, TransactionalSpoutOutputCollector collector);
}
