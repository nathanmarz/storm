package backtype.storm.testing;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;


public class TrackerAggregator implements IRichBolt {
    public static final Map<String, TrackStats> _stats = new HashMap<String, TrackStats>();

    public static String TRACK_STREAM = "TrackerAggregator/track";

    TopologyContext _context;

    public static class TrackStats {
        int spoutEmitted = 0;
        int transferred = 0;
        int processed = 0;
    }

    String _id;

    public TrackerAggregator() {
        _id = UUID.randomUUID().toString();
        _stats.put(_id, new TrackStats());
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _context = context;
    }

    public int getSpoutEmitted() {
        synchronized(_stats) {
            return _stats.get(_id).spoutEmitted;
        }
    }

    public int getTransferred() {
        synchronized(_stats) {
            return _stats.get(_id).transferred;
        }
    }

    public int getProcessed() {
        synchronized(_stats) {
            return _stats.get(_id).processed;
        }
    }

    public void execute(Tuple input) {
        int processed = input.getInteger(0);
        int transferred = input.getInteger(1);
        int spoutEmitted = input.getInteger(2);
        synchronized(_stats) {
            TrackStats stats = _stats.get(_id);
            stats.processed+=processed;
            stats.spoutEmitted+=spoutEmitted;
            stats.transferred+=transferred;
        }
    }

    public void cleanup() {        
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
