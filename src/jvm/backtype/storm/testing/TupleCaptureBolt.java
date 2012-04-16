package backtype.storm.testing;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;


public class TupleCaptureBolt implements IRichBolt {
    public static transient Map<String, Map<String, List<FixedTuple>>> emitted_tuples = new HashMap<String, Map<String, List<FixedTuple>>>();

    private String _name;
    private OutputCollector _collector;

    public TupleCaptureBolt() {
        _name = UUID.randomUUID().toString();
        emitted_tuples.put(_name, new HashMap<String, List<FixedTuple>>());
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    public void execute(Tuple input) {
        String component = input.getSourceComponent();
        Map<String, List<FixedTuple>> captured = emitted_tuples.get(_name);
        if(!captured.containsKey(component)) {
           captured.put(component, new ArrayList<FixedTuple>());
        }
        captured.get(component).add(new FixedTuple(input.getSourceStreamId(), input.getValues()));
        _collector.ack(input);
    }

    public Map<String, List<FixedTuple>> getResults() {
        return emitted_tuples.get(_name);
    }

    public void cleanup() {
    }
    
    public Map<String, List<FixedTuple>> getAndRemoveResults() {
        return emitted_tuples.remove(_name);
    }

    public Map<String, List<FixedTuple>> getAndClearResults() {
        Map<String, List<FixedTuple>> ret = new HashMap<String, List<FixedTuple>>(emitted_tuples.get(_name));
        emitted_tuples.get(_name).clear();
        return ret;
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
