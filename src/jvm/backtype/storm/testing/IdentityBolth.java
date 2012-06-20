package backtype.storm.testing;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicbolth;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class Identitybolth extends BaseBasicbolth {
    Fields _fields;
    
    public Identitybolth(Fields fields) {
        _fields = fields;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        collector.emit(input.getValues());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(_fields);
    }    
}
