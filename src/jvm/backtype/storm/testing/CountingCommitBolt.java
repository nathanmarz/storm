package backtype.storm.testing;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.topology.base.BaseCommitterBolt;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Map;

public class CountingCommitBolt extends BaseCommitterBolt {
    TransactionAttempt _id;
    int _count = 0;
    
    @Override
    public void prepare(Map conf, TopologyContext context, TransactionAttempt id) {
        _id = id;
    }

    @Override
    public void execute(Tuple tuple) {
        _count++;
    }

    @Override
    public void commit(BatchOutputCollector collector) {
        collector.emit(new Values(_id, _count));        
    }   

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tx", "count"));
    }
    
}
