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

    public TransactionalSpoutBatchExecutor(ITransactionalSpout spout) {
        _spout = spout;
    }
    
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = new TransactionalOutputCollectorImpl(collector);
        _spout.open(conf, context);
    }

    @Override
    public void execute(Tuple input) {
        _collector.setAnchor(input);
        _spout.emitBatch((TransactionAttempt) input.getValue(0), _collector);
        _collector.ack();
    }

    @Override
    public void cleanup() {
        _spout.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        _spout.declareOutputFields(declarer);
    }    
}
