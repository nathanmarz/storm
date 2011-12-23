package backtype.storm.transactional;

import backtype.storm.task.IBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import java.util.HashMap;
import java.util.Map;

public class TransactionalBoltExecutor implements IBolt {
    byte[] _boltSer;
    Map<Long, Map<TransactionAttempt, ITransactionalBolt>> openTransactions = new HashMap<Long, Map<TransactionAttempt, ITransactionalBolt>>();
    Map _conf;
    TopologyContext _context;
    OutputCollector _collector;
    
    public TransactionalBoltExecutor(ITransactionalBolt bolt) {
        _boltSer = Utils.serialize(bolt);
    }
    
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _conf = conf;
        _context = context;
        _collector = collector;
    }

    @Override
    public void execute(Tuple input) {        
        // check if it's a commit tuple... and wrap in a transaction tuple and call commit
        // // input.getSourceComponent() is from a transaction spout
        // otherwise, get the transaction id from the first field and pass it along (creating 
        // transactionbolt if necessary
        // ITransactionalBolt bolt = Utils.deserialize(_boltSer);
        // acking the transaction tuple is the "OK to commit message the spout needs!!!"
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void cleanup() {
        openTransactions.clear();
    }    
}
