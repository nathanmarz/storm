package backtype.storm.testing;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.RegisteredGlobalState;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


public class BoltTracker implements IRichBolt {
    IRichBolt _delegate;
    String _trackId;

    public BoltTracker(IRichBolt delegate, String id) {
        _delegate = delegate;
        _trackId = id;
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _delegate.prepare(stormConf, context, collector);
    }

    public void execute(Tuple input) {
        _delegate.execute(input);
        Map stats = (Map) RegisteredGlobalState.getState(_trackId);
        ((AtomicInteger) stats.get("processed")).incrementAndGet();
    }

    public void cleanup() {
        _delegate.cleanup();
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        _delegate.declareOutputFields(declarer);
    }
}
