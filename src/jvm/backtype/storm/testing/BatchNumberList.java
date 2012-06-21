package backtype.storm.testing;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class BatchNumberList extends BaseBatchBolt {

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "list"));
    }

    String _wordComponent;
    
    public BatchNumberList(String wordComponent) {
        _wordComponent = wordComponent;
    }
    
    String word = null;
    List<Integer> intSet = new ArrayList<Integer>();
    BatchOutputCollector _collector;
    
    
    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        if(tuple.getSourceComponent().equals(_wordComponent)) {
            this.word = tuple.getString(1);
        } else {
            intSet.add(tuple.getInteger(1));
        }
    }

    @Override
    public void finishBatch() {
        if(word!=null) {
            Collections.sort(intSet);
            _collector.emit(new Values(word, intSet));
        }
    }
    
}
