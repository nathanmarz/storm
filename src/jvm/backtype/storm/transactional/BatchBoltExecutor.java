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

public class BatchBoltExecutor implements IRichBolt, FinishedCallback, TimeoutCallback {
    public static Logger LOG = Logger.getLogger(BatchBoltExecutor.class);    

    byte[] _boltSer;
    Map<TransactionAttempt, IBatchBolt> _openTransactions;
    Map _conf;
    TopologyContext _context;
    TransactionalOutputCollectorImpl _collector;
    
    public BatchBoltExecutor(IBatchBolt bolt) {
        _boltSer = Utils.serialize(bolt);
    }
    
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _conf = conf;
        _context = context;
        _collector = new TransactionalOutputCollectorImpl(collector);
        _openTransactions = new HashMap<TransactionAttempt, IBatchBolt>();
    }

    @Override
    public void execute(Tuple input) {
        TransactionAttempt attempt = (TransactionAttempt) input.getValue(0);
        IBatchBolt bolt = getTransactionalBolt(attempt);
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
        TransactionAttempt attempt = (TransactionAttempt) tup.getId();
        IBatchBolt bolt = getTransactionalBolt(attempt);
        _openTransactions.remove(attempt);
        try {
            bolt.finishBatch();
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
        newTransactionalBolt().declareOutputFields(declarer);
    }
    
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return newTransactionalBolt().getComponentConfiguration();
    }
    
    private IBatchBolt getTransactionalBolt(TransactionAttempt attempt) {
        IBatchBolt bolt = _openTransactions.get(attempt);
        if(bolt==null) {
            bolt = newTransactionalBolt();
            bolt.prepare(_conf, _context, _collector, attempt);
            _openTransactions.put(attempt, bolt);            
        }
        return bolt;
    }
    
    private IBatchBolt newTransactionalBolt() {
        return (IBatchBolt) Utils.deserialize(_boltSer);
    }
}
