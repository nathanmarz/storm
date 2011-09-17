package backtype.storm.testing;

import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.util.List;
import java.util.Map;


public class SpoutTracker implements IRichSpout {
    IRichSpout _delegate;
    SpoutTrackOutputCollector _tracker;


    private static class SpoutTrackOutputCollector implements ISpoutOutputCollector {
        public int transferred = 0;
        public int emitted = 0;
        public SpoutOutputCollector _collector;

        public SpoutTrackOutputCollector(SpoutOutputCollector collector) {
            _collector = collector;
        }

        public List<Integer> emit(int streamId, List<Object> tuple, Object messageId) {
            List<Integer> ret = _collector.emit(streamId, tuple, messageId);
            emitted++;
            transferred += ret.size();
            if(messageId!=null) transferred++;
            //for the extra ack on branching or finishing
            if(ret.size()!=1) {
                transferred += 1;
            }

            return ret;
        }

        public void emitDirect(int taskId, int streamId, List<Object> tuple, Object messageId) {
            _collector.emitDirect(taskId, streamId, tuple, messageId);
            emitted++;
            transferred++;
            if(messageId!=null) transferred++;
        }

    }


    public SpoutTracker(IRichSpout delegate) {
        _delegate = delegate;
    }

    public boolean isDistributed() {
        return _delegate.isDistributed();
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _tracker = new SpoutTrackOutputCollector(collector);
        _delegate.open(conf, context, new SpoutOutputCollector(_tracker));
    }

    public void close() {
        _delegate.close();
    }

    public void nextTuple() {
        int curr = _tracker.transferred;
        int currEmitted = _tracker.emitted;
        _delegate.nextTuple();
        int transferred = _tracker.transferred - curr;
        if(transferred>0) {
            _tracker._collector.emit(TrackerAggregator.TRACK_STREAM, new Values(0, transferred, _tracker.emitted - currEmitted));
        }
    }

    public void ack(Object msgId) {
        _delegate.ack(msgId);
        _tracker._collector.emit(TrackerAggregator.TRACK_STREAM, new Values(1, 0, 0));
    }

    public void fail(Object msgId) {
        _delegate.fail(msgId);
        //TODO: this is not strictly correct, since the message could have just timed out
        _tracker._collector.emit(TrackerAggregator.TRACK_STREAM, new Values(1, 0, 0));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        _delegate.declareOutputFields(declarer);
        declarer.declareStream(TrackerAggregator.TRACK_STREAM, new Fields("processed", "transferred", "spoutEmitted"));
    }

}
