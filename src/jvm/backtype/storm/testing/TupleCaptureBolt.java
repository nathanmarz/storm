package backtype.storm.testing;

import backtype.storm.task.IBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TupleCaptureBolt implements IBolt {
    public static transient Map<String, Map<String, List<FixedTuple>>> emitted_tuples = new HashMap<String, Map<String, List<FixedTuple>>>();

    private String _name;
    private Map<String, List<FixedTuple>> _results = null;
    private OutputCollector _collector;

    public TupleCaptureBolt(String name) {
        _name = name;
        emitted_tuples.put(name, new HashMap<String, List<FixedTuple>>());
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
        if(_results==null) {
            _results = emitted_tuples.remove(_name);
        }
        return _results;
    }

    public void cleanup() {
    }

}
