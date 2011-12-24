package backtype.storm.transactional;

import backtype.storm.task.TopologyContext;
import java.util.Map;

public interface ITransactionState {
    void open(Map conf, TopologyContext context);
    void close();
    void setTransactionId(int txid);
    int getTransactionId();    
}
