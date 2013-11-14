package backtype.storm.transactional;

import backtype.storm.coordination.BatchOutputCollectorImpl;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import java.math.BigInteger;
import java.util.Map;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionalSpoutBatchExecutor implements IRichBolt {
    public static Logger LOG = LoggerFactory.getLogger(TransactionalSpoutBatchExecutor.class);    

    BatchOutputCollectorImpl _collector;
    ITransactionalSpout _spout;
    ITransactionalSpout.Emitter _emitter;
    
    TreeMap<BigInteger, TransactionAttempt> _activeTransactions = new TreeMap<BigInteger, TransactionAttempt>();

    public TransactionalSpoutBatchExecutor(ITransactionalSpout spout) {
        _spout = spout;
    }
    
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = new BatchOutputCollectorImpl(collector);
        _emitter = _spout.getEmitter(conf, context);
    }

    @Override
    public void execute(Tuple input) {
        TransactionAttempt attempt = (TransactionAttempt) input.getValue(0);
        try {
            if(input.getSourceStreamId().equals(TransactionalSpoutCoordinator.TRANSACTION_COMMIT_STREAM_ID)) {
                if(attempt.equals(_activeTransactions.get(attempt.getTransactionId()))) {
                    ((ICommitterTransactionalSpout.Emitter) _emitter).commit(attempt);
                    _activeTransactions.remove(attempt.getTransactionId());
                    _collector.ack(input);
                } else {
                    _collector.fail(input);
                }
            } else { 
                _emitter.emitBatch(attempt, input.getValue(1), _collector);
                _activeTransactions.put(attempt.getTransactionId(), attempt);
                _collector.ack(input);
                BigInteger committed = (BigInteger) input.getValue(2);
                if(committed!=null) {
                    // valid to delete before what's been committed since 
                    // those batches will never be accessed again
                    _activeTransactions.headMap(committed).clear();
                    _emitter.cleanupBefore(committed);
                }
            }
        } catch(FailedException e) {
            LOG.warn("Failed to emit batch for transaction", e);
            _collector.fail(input);
        }
    }

    @Override
    public void cleanup() {
        _emitter.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        _spout.declareOutputFields(declarer);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return _spout.getComponentConfiguration();
    }
}
