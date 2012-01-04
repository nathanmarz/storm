package backtype.storm.transactional;

import backtype.storm.task.TopologyContext;
import java.util.List;
import java.util.Map;

//TODO: what if i just force to use zookeeper for this? how to serialize transaction meta? kryo?
public interface ITransactionalState {
    void open(Map conf, TopologyContext context);
    void close();
    int getCurrentTransaction();
    void setCurrentTransaction(int txid);
    Object getTransactionMeta(int txid);
    List<Integer> getMetaTransactions();
    void deleteTransactionMeta(int txid);
}
