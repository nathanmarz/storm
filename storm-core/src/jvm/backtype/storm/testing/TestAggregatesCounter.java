package backtype.storm.testing;

import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Fields;
import java.util.Map;
import backtype.storm.task.TopologyContext;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static backtype.storm.utils.Utils.tuple;


public class TestAggregatesCounter extends BaseRichBolt {
    public static Logger LOG = LoggerFactory.getLogger(TestWordCounter.class);

    Map<String, Integer> _counts;
    OutputCollector _collector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        _counts = new HashMap<String, Integer>();
    }

    public void execute(Tuple input) {
        String word = (String) input.getValues().get(0);
        int count = (Integer) input.getValues().get(1);
        _counts.put(word, count);
        int globalCount = 0;
        for(String w: _counts.keySet()) {
            globalCount+=_counts.get(w);
        }
        _collector.emit(tuple(globalCount));
        _collector.ack(input);
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("agg-global"));
    }
}