package backtype.storm.transactional;

import backtype.storm.task.TopologyContext;
import java.util.Map;


public interface ICommitterTransactionalSpout<X> extends ITransactionalSpout<X> {
    public interface Emitter extends ITransactionalSpout.Emitter {
        void commit(TransactionAttempt attempt);
    } 
    
    @Override
    public Emitter getEmitter(Map conf, TopologyContext context);    
}
