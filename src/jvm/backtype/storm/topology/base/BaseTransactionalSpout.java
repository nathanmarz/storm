package backtype.storm.topology.base;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.transactional.ITransactionalSpout;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.transactional.TransactionalOutputCollector;
import java.util.Map;

public abstract class BaseTransactionalSpout implements ITransactionalSpout {
    String _id;
    
    public BaseTransactionalSpout(String id) {
        _id = id;
    }
    
    @Override
    public void open(Map conf, TopologyContext context) {
    }

    @Override
    public void close() {        
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public String getId() {
        return _id;
    }    
}
