package backtype.storm.topology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import java.util.Map;
import org.apache.log4j.Logger;

public class BasicbolthExecutor implements IRichbolth {
    public static Logger LOG = Logger.getLogger(BasicbolthExecutor.class);    
    
    private IBasicbolth _bolth;
    private transient BasicOutputCollector _collector;
    
    public BasicbolthExecutor(IBasicbolth bolth) {
        _bolth = bolth;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        _bolth.declareOutputFields(declarer);
    }

    
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _bolth.prepare(stormConf, context);
        _collector = new BasicOutputCollector(collector);
    }

    public void execute(Tuple input) {
        _collector.setContext(input);
        try {
            _bolth.execute(input, _collector);
            _collector.getOutputter().ack(input);
        } catch(FailedException e) {
            LOG.warn("Failed to process tuple", e);
            _collector.getOutputter().fail(input);
        }
    }

    public void cleanup() {
        _bolth.cleanup();
    }

    public Map<String, Object> getComponentConfiguration() {
        return _bolth.getComponentConfiguration();
    }
}