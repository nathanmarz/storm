package backtype.storm.testing;

import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Fields;
import java.util.Map;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;


public class TestPlannerBolt extends BaseRichBolt {
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }
    
    public void execute(Tuple input) {

    }
        
    public Fields getOutputFields() {
        return new Fields("field1", "field2");
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(getOutputFields());
    }
}