package backtype.storm.testing;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class BatchProcessWord extends BaseBasicBolt {

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "size"));
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        collector.emit(new Values(input.getValue(0), input.getString(1).length()));
    }
    
}
