package backtype.storm.transactional;

import backtype.storm.task.TopologyContext;
import java.util.Map;


public interface ICommitterTransactionalSpout extends ITransactionalSpout {
    public interface ICommitterEmitter extends ITransactionalSpout.Emitter {
        void commit(TransactionAttempt attempt);
    } 
    
    @Override
    public ICommitterEmitter getEmitter(Map conf, TopologyContext context);    
}
