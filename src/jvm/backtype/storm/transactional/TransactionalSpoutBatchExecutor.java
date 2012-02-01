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
import org.apache.log4j.Logger;

public class TransactionalSpoutBatchExecutor implements IRichBolt {
    public static Logger LOG = Logger.getLogger(TransactionalSpoutBatchExecutor.class);    

    BatchOutputCollectorImpl _collector;
    ITransactionalSpout _spout;
    ITransactionalSpout.Emitter _emitter;

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
            _emitter.emitBatch(attempt, input.getValue(1), _collector);
            _collector.ack(input);
        } catch(FailedException e) {
            LOG.warn("Failed to emit batch for transaction", e);
            _collector.fail(input);
        }
        // this is valid here because the batch has been successfully emitted, 
        // so we can safely delete metadata for prior transactions
        _emitter.cleanupBefore((BigInteger) input.getValue(2));
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
