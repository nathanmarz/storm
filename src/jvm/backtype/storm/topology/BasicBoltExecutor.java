package backtype.storm.topology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import java.util.HashMap;
import java.util.Map;

public class BasicBoltExecutor implements IRichBolt {
    
    private IBasicBolt _bolt;
    private transient BasicOutputCollector _collector;
    
    public BasicBoltExecutor(IBasicBolt bolt) {
        _bolt = bolt;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        _bolt.declareOutputFields(declarer);
    }

    
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _bolt.prepare(stormConf, context);
        _collector = new BasicOutputCollector(collector);
    }

    public void execute(Tuple input) {
        _collector.setContext(input);
        _bolt.execute(input, _collector);
        _collector.getOutputter().ack(input);
    }

    public void cleanup() {
        _bolt.cleanup();
    }

    public Map<String, Object> getComponentConfiguration() {
        return _bolt.getComponentConfiguration();
    }
}