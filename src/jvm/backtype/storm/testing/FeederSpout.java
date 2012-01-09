package backtype.storm.testing;

import backtype.storm.topology.OutputFieldsDeclarer;
import java.util.Map;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.InprocMessaging;
import backtype.storm.utils.Utils;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;


public class FeederSpout implements IRichSpout {
    private int _id;
    private Fields _outFields;
    private SpoutOutputCollector _collector;
    private AckFailDelegate _ackFailDelegate;

    public FeederSpout(Fields outFields) {
        _id = InprocMessaging.acquireNewPort();
        _outFields = outFields;
    }

    public void setAckFailDelegate(AckFailDelegate d) {
        _ackFailDelegate = d;
    }
    
    public void feed(List<Object> tuple) {
        InprocMessaging.sendMessage(_id, tuple);
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
    }

    public void close() {

    }

    public void nextTuple() {
        List<Object> tuple = (List<Object>) InprocMessaging.pollMessage(_id);
        if(tuple!=null) {
            _collector.emit(tuple, UUID.randomUUID().toString());
        } else {
            Utils.sleep(10);                
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

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return new HashMap<String, Object>();
    }
}