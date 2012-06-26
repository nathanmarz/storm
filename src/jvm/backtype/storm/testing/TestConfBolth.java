package backtype.storm.testing;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicbolth;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Map;


public class TestConfbolth extends BaseBasicbolth {
    Map<String, Object> _componentConf;
    Map<String, Object> _conf;

    public TestConfbolth() {
        this(null);
    }
        
    public TestConfbolth(Map<String, Object> componentConf) {
        _componentConf = componentConf;
    }        

    @Override
    public void prepare(Map conf, TopologyContext context) {
        _conf = conf;
    }    
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("conf", "value"));
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String name = input.getString(0);
        collector.emit(new Values(name, _conf.get(name)));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return _componentConf;
    }    
}
