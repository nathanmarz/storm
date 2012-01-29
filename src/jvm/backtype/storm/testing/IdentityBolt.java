package backtype.storm.testing;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class IdentityBolt extends BaseBasicBolt {
    Fields _fields;
    
    public IdentityBolt(Fields fields) {
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
