package backtype.storm.testing;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import java.util.ArrayList;
import java.util.List;


public class PrepareBatchBolt extends BaseBasicBolt {
    Fields _outFields;
    
    public PrepareBatchBolt(Fields outFields) {
        _outFields = outFields;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(_outFields);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        long id = Utils.secureRandomLong();
        List<Object> toEmit = new ArrayList<Object>();
        toEmit.add(id);
        toEmit.addAll(input.getValues());
        collector.emit(toEmit);
    }
    
    
}
