package backtype.storm.topology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import java.util.Map;
import org.apache.log4j.Logger;

public class BasicBoltExecutor implements IRichBolt {
    public static Logger LOG = Logger.getLogger(BasicBoltExecutor.class);    
    
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
        try {
            _bolt.execute(input, _collector);
            _collector.getOutputter().ack(input);
        } catch(FailedException e) {
            LOG.warn("Failed to process tuple", e);
            _collector.getOutputter().fail(input);
        }
    }

    public void cleanup() {
        _bolt.cleanup();
    }

    public Map<String, Object> getComponentConfiguration() {
        return _bolt.getComponentConfiguration();
    }
}