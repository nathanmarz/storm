package backtype.storm.transactional;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.IBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.TimeCacheMap;
import backtype.storm.utils.Utils;
import java.util.Map;

// TODO: would be helpful to have a non-committing executor that just helps with keeping state
// separated by transaction attempt
public class TransactionalBoltExecutor implements IBolt {
    byte[] _boltSer;
    TimeCacheMap<TransactionAttempt, ITransactionalBolt> _openTransactions;
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
        _openTransactions = new TimeCacheMap<TransactionAttempt, ITransactionalBolt>(Utils.getInt(conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS)));
    }

    @Override
    public void execute(Tuple input) {
        TransactionAttempt attempt = (TransactionAttempt) input.getValue(0);
        if(!_openTransactions.containsKey(attempt)) {
            // TODO: might need to optimize this with a factory
            ITransactionalBolt bolt = (ITransactionalBolt) Utils.deserialize(_boltSer);
            bolt.prepare(_conf, _context, attempt.getTransactionId());
            _openTransactions.put(attempt, bolt);
        }
        ITransactionalBolt bolt = _openTransactions.get(attempt);
        if(bolt!=null) {
            if(input.getSourceStreamId().equals(Constants.TRANSACTION_COMMIT_STREAM_ID)) {
                bolt.commit(new TransactionTuple(input), _collector);
                _openTransactions.remove(attempt);
            } else {
                bolt.execute(input);
            }
        }
        _collector.ack(input);
    }

    @Override
    public void cleanup() {
    }    
}
