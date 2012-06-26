package backtype.storm.testing;

import backtype.storm.topology.base.BaseRichbolth;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Fields;
import java.util.Map;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;


public class TestGlobalCount extends BaseRichbolth {
    public static Logger LOG = Logger.getLogger(TestWordCounter.class);

    private int _count;
    OutputCollector _collector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        _count = 0;
    }

    public void execute(Tuple input) {
        _count++;
        _collector.emit(input, new Values(_count));
        _collector.ack(input);
    }

    public void cleanup() {

    }

    public Fields getOutputFields() {
        return new Fields("global-count");
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("global-count"));
    }
}
