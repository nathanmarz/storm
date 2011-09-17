package backtype.storm.testing;

import backtype.storm.task.IBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class BoltTracker implements IRichBolt {
    IBolt _delegate;

    BoltTrackerOutputCollector _trackCollector;

    private static class BoltTrackerOutputCollector extends OutputCollector {
        public OutputCollector _collector;

        public int transferred = 0;

        public BoltTrackerOutputCollector(OutputCollector collector) {
            _collector = collector;
        }


        public List<Integer> emit(int streamId, List<Tuple> anchors, List<Object> tuple) {
            List<Integer> ret = _collector.emit(streamId, anchors, tuple);
            transferred += ret.size();

            //for the extra ack on branching or finishing
            if(ret.size()!=1) {
                Set<Long> anchorIds = new HashSet<Long>();
                for(Tuple t: anchors) {
                    anchorIds.addAll(t.getMessageId().getAnchors());
                }
                transferred += anchorIds.size();
            }
            return ret;
        }

        public void emitDirect(int taskId, int streamId, List<Tuple> anchors, List<Object> tuple) {
            _collector.emitDirect(taskId, streamId, anchors, tuple);
            transferred++;
        }

        public void ack(Tuple input) {
            _collector.ack(input);
            transferred+=input.getMessageId().getAnchors().size();
        }

        public void fail(Tuple input) {
            _collector.fail(input);
            transferred+=input.getMessageId().getAnchors().size();
        }

        public void reportError(Throwable error) {
            _collector.reportError(error);
        }

    }

    /*
     * IBolt so it can be used to wrap acker bolts (which don't declare streams because they're implicit)
     */
    public BoltTracker(IBolt delegate) {
        _delegate = delegate;
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _trackCollector = new BoltTrackerOutputCollector(collector);
        _delegate.prepare(stormConf, context, _trackCollector);
    }

    public void execute(Tuple input) {
        int currTransferred = _trackCollector.transferred;
        _delegate.execute(input);
        _trackCollector._collector.emit(TrackerAggregator.TRACK_STREAM,
                new Values(1, _trackCollector.transferred - currTransferred, 0));
    }

    public void cleanup() {
        _delegate.cleanup();
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if(_delegate instanceof IRichBolt) {
            ((IRichBolt)_delegate).declareOutputFields(declarer);
        }
        declarer.declareStream(TrackerAggregator.TRACK_STREAM, new Fields("processed", "transferred", "spoutEmitted"));
    }
}
