package backtype.storm.transactional;

import java.util.Map;

public interface ITransactionalSpout {
    void open(Map conf);
    void close();
    void setTransactionId(long txid);
    long getTransactionId();
    void emitBatch(TransactionAttempt tx, TransactionalSpoutOutputCollector collector);
}
