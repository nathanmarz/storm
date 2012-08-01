package storm.trident.spout;

import backtype.storm.task.TopologyContext;
import storm.trident.topology.TransactionAttempt;
import java.util.Map;

public interface ICommitterTridentSpout<X> extends ITridentSpout<X> {
    public interface Emitter extends ITridentSpout.Emitter {
        void commit(TransactionAttempt attempt);
    } 
    
    @Override
    public Emitter getEmitter(String txStateId, Map conf, TopologyContext context);    
}