package backtype.storm.spout;

import backtype.storm.task.TopologyContext;
import java.util.Map;


public class NoOpSpout implements ISpout {
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    }

    @Override
    public void close() {
    }

    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }

    @Override
    public void nextTuple() {
    }

    @Override
    public void ack(Object msgId) {
    }

    @Override
    public void fail(Object msgId) {
    }
    
}
