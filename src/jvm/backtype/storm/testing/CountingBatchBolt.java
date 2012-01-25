package backtype.storm.testing;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.transactional.TransactionalOutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Map;

public class CountingBatchBolt extends BaseBatchBolt {
    public static final String BATCH_STREAM = "batch";
    
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
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(BATCH_STREAM, new Fields("tx", "count"));
    }
    
}
