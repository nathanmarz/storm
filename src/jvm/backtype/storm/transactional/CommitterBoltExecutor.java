package backtype.storm.transactional;

import backtype.storm.coordination.IBatchBolt;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import java.util.Map;


public class CommitterBoltExecutor implements IBatchBolt  {
    ICommitterBolt _delegate;
    BatchOutputCollector _collector;

    public CommitterBoltExecutor(ICommitterBolt delegate) {
        _delegate = delegate;
    }
    
    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
        _delegate.prepare(conf, context, (TransactionAttempt) id);
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        _delegate.execute(tuple);        
    }

    @Override
    public void finishBatch() {
        _delegate.commit(_collector);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        _delegate.declareOutputFields(declarer);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return _delegate.getComponentConfiguration();
    }
 
}
