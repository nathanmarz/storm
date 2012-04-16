package backtype.storm.testing;

import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.utils.RegisteredGlobalState;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


public class SpoutTracker extends BaseRichSpout {
    IRichSpout _delegate;
    SpoutTrackOutputCollector _tracker;
    String _trackId;


    private class SpoutTrackOutputCollector implements ISpoutOutputCollector {
        public int transferred = 0;
        public int emitted = 0;
        public SpoutOutputCollector _collector;

        public SpoutTrackOutputCollector(SpoutOutputCollector collector) {
            _collector = collector;
        }
        
        private void recordSpoutEmit() {
            Map stats = (Map) RegisteredGlobalState.getState(_trackId);
            ((AtomicInteger) stats.get("spout-emitted")).incrementAndGet();
            
        }

        public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
            List<Integer> ret = _collector.emit(streamId, tuple, messageId);
            recordSpoutEmit();
            return ret;
        }

        public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
            _collector.emitDirect(taskId, streamId, tuple, messageId);
            recordSpoutEmit();
        }

        @Override
        public void reportError(Throwable error) {
        	_collector.reportError(error);
        }
    }


    public SpoutTracker(IRichSpout delegate, String trackId) {
        _delegate = delegate;
        _trackId = trackId;
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _tracker = new SpoutTrackOutputCollector(collector);
        _delegate.open(conf, context, new SpoutOutputCollector(_tracker));
    }

    public void close() {
        _delegate.close();
    }

    public void nextTuple() {
        _delegate.nextTuple();
    }

    public void ack(Object msgId) {
        _delegate.ack(msgId);
        Map stats = (Map) RegisteredGlobalState.getState(_trackId);
        ((AtomicInteger) stats.get("processed")).incrementAndGet();
    }

    public void fail(Object msgId) {
        _delegate.fail(msgId);
        Map stats = (Map) RegisteredGlobalState.getState(_trackId);
        ((AtomicInteger) stats.get("processed")).incrementAndGet();        
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        _delegate.declareOutputFields(declarer);
    }

}
