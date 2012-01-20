package backtype.storm.transactional;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import java.util.Map;

public class TransactionalSpoutBatchExecutor implements IRichBolt {    
    TransactionalOutputCollectorImpl _collector;
    ITransactionalSpout _spout;
    ITransactionalSpout.Emitter _emitter;

    public TransactionalSpoutBatchExecutor(ITransactionalSpout spout) {
        _spout = spout;
    }
    
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = new TransactionalOutputCollectorImpl(collector);
        _emitter = _spout.getEmitter(conf, context);
    }

    @Override
    public void execute(Tuple input) {
        TransactionAttempt attempt = (TransactionAttempt) input.getValue(0);
        if(input.getSourceStreamId().equals(TransactionalSpoutCoordinator.TRANSACTION_COMMIT_STREAM_ID)) {
            _collector.ack(input);
            // can ack before since it doesn't matter if this fails, since it will just cleanup the next commit
            _emitter.cleanupBefore(attempt.getTransactionId());
        } else {
            _emitter.emitBatch(attempt, input.getValue(1), _collector);
            _collector.ack(input);
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
