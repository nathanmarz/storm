package backtype.storm.testing;

import backtype.storm.topology.OutputFieldsDeclarer;
import java.util.Map;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;


public class TestPlannerSpout implements IRichSpout {
    boolean _isDistributed;
    
    public TestPlannerSpout(boolean isDistributed) {
        _isDistributed = isDistributed;
    }
    
    public boolean isDistributed() {
        return _isDistributed;
    }
    
    public Fields getOutputFields() {
        return new Fields("field1", "field2");
    }

    
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        
    }
    
    public void close() {
        
    }
    
    public void nextTuple() {
        Utils.sleep(100);
    }
    
    public void ack(Object msgId){
        
    }

    public void fail(Object msgId){
        
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(getOutputFields());
    }
    
}