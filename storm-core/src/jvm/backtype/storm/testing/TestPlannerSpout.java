package backtype.storm.testing;

import backtype.storm.Config;
import backtype.storm.topology.OutputFieldsDeclarer;
import java.util.Map;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import java.util.HashMap;


public class TestPlannerSpout extends BaseRichSpout {
    boolean _isDistributed;
    Fields _outFields;
    
    public TestPlannerSpout(Fields outFields, boolean isDistributed) {
        _isDistributed = isDistributed;
        _outFields = outFields;
    }

    public TestPlannerSpout(boolean isDistributed) {
        this(new Fields("field1", "field2"), isDistributed);
    }
        
    public TestPlannerSpout(Fields outFields) {
        this(outFields, true);
    }
    
    public Fields getOutputFields() {
        return _outFields;
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

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> ret = new HashMap<String, Object>();
        if(!_isDistributed) {
            ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
        }
        return ret;
    }       
}