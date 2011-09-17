package backtype.storm.testing;

import backtype.storm.topology.OutputFieldsDeclarer;
import java.util.Map;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import java.util.List;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;


public class FeederSpout implements IRichSpout {

    public transient static Map<String, LinkedBlockingQueue<List<Object>>> _feeds = new HashMap<String, LinkedBlockingQueue<List<Object>>>();

    private String _id;
    private Fields _outFields;
    private SpoutOutputCollector _collector;
    private AckFailDelegate _ackFailDelegate;

    public FeederSpout(Fields outFields) {
        _id = UUID.randomUUID().toString();
        _feeds.put(_id, new LinkedBlockingQueue<List<Object>>());
        _outFields = outFields;
    }

    public void setAckFailDelegate(AckFailDelegate d) {
        _ackFailDelegate = d;
    }
    
    public void feed(List<Object> tuple) {
        _feeds.get(_id).add(tuple);
    }

    public boolean isDistributed() {
        return true;
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
    }

    public void close() {

    }

    public void nextTuple() {
        List<Object> tuple = _feeds.get(_id).poll();
        if(tuple!=null) {
            _collector.emit(tuple, UUID.randomUUID().toString());
        } else {
            Utils.sleep(100);                
        }
    }

    public void ack(Object msgId){
        if(_ackFailDelegate!=null) {
            _ackFailDelegate.ack(msgId);
        }
    }

    public void fail(Object msgId){
        if(_ackFailDelegate!=null) {
            _ackFailDelegate.fail(msgId);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(_outFields);
    }
}