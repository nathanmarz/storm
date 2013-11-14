package backtype.storm.testing;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class BatchRepeatA extends BaseBasicBolt {  
    
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
       Object id = input.getValue(0);
       String word = input.getString(1);
       for(int i=0; i<word.length(); i++) {
            if(word.charAt(i) == 'a') {
                collector.emit("multi", new Values(id, word.substring(0, i)));
            }
        }
        collector.emit("single", new Values(id, word));
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("multi", new Fields("id", "word"));
        declarer.declareStream("single", new Fields("id", "word"));
    }

}
