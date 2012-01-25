package backtype.storm.transactional;

import backtype.storm.coordination.CoordinatedBolt.FinishedCallback;
import backtype.storm.coordination.CoordinatedBolt.TimeoutCallback;
import backtype.storm.coordination.FinishedTuple;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;


public class CommitterBoltExecutor implements IRichBolt, FinishedCallback, TimeoutCallback  {
    public static Logger LOG = Logger.getLogger(CommitterBoltExecutor.class);    

    byte[] _boltSer;
    Map<TransactionAttempt, ICommitterBolt> _openTransactions;
    Map _conf;
    TopologyContext _context;
    TransactionalOutputCollectorImpl _collector;
    
    public CommitterBoltExecutor(ICommitterBolt bolt) {
        _boltSer = Utils.serialize(bolt);
    }
    
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _conf = conf;
        _context = context;
        _collector = new TransactionalOutputCollectorImpl(collector);
        _openTransactions = new HashMap<TransactionAttempt, ICommitterBolt>();
    }

    @Override
    public void execute(Tuple input) {
        TransactionAttempt attempt = (TransactionAttempt) input.getValue(0);
        ICommitterBolt bolt = getCommitterBolt(attempt);
        
        try {
             bolt.execute(input);
            _collector.ack(input);
        } catch(FailedTransactionException e) {
            LOG.warn("Failed to process tuple in transaction", e);
            _collector.fail(input);                
        }
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void finishedId(FinishedTuple tup) {
        // TODO: what about acking? does it just work because the commit tuple must be the last tuple so
        // everything gets anchored to it
        TransactionAttempt attempt = (TransactionAttempt) tup.getId();
        ICommitterBolt bolt = getCommitterBolt(attempt);
        _openTransactions.remove(attempt);
        try {
            bolt.commit(_collector);
        } catch(FailedTransactionException e) {
            // TODO: need to be able to fail from here... need support from coordinatedbolt
            throw e;
        }
    }

    @Override
    public void failId(Object attempt) {
        _openTransactions.remove((TransactionAttempt) attempt);        
    }    
    

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        newCommitterBolt().declareOutputFields(declarer);
    }
    
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return newCommitterBolt().getComponentConfiguration();
    }
    
    private ICommitterBolt getCommitterBolt(TransactionAttempt attempt) {
        ICommitterBolt bolt = _openTransactions.get(attempt);
        if(bolt==null) {
            bolt = newCommitterBolt();
            bolt.prepare(_conf, _context, attempt);
            _openTransactions.put(attempt, bolt);            
        }
        return bolt;
    }
    
    private ICommitterBolt newCommitterBolt() {
        return (ICommitterBolt) Utils.deserialize(_boltSer);
    }    
}
