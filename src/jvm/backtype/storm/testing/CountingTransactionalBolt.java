package backtype.storm.testing;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.ICommittable;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.transactional.TransactionalOutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Map;

public class CountingTransactionalBolt extends BaseTransactionalBolt implements ICommittable {
    public static final String BATCH_STREAM = "batch";
    public static final String COMMIT_STREAM = "commit";
    
    TransactionalOutputCollector _collector;
    TransactionAttempt _attempt;
    int _count = 0;
    
    @Override
    public void prepare(Map conf, TopologyContext context, TransactionalOutputCollector collector, TransactionAttempt attempt) {
        _collector = collector;
        _attempt = attempt;
    }

    @Override
    public void execute(Tuple tuple) {
        _count++;
    }

    @Override
    public void finishBatch() {
        _collector.emit(BATCH_STREAM, new Values(_attempt, _count));        
    }

    @Override
    public void commit() {
        _collector.emit(COMMIT_STREAM, new Values(_attempt, _count));        
    }    

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(BATCH_STREAM, new Fields("tx", "count"));
        declarer.declareStream(COMMIT_STREAM, new Fields("tx", "count"));
    }
    
}
