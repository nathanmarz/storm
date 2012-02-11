package backtype.storm.testing;

import backtype.storm.task.IBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.RegisteredGlobalState;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


public class NonRichBoltTracker implements IBolt {
    IBolt _delegate;
    String _trackId;

    public NonRichBoltTracker(IBolt delegate, String id) {
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
}
