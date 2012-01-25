package backtype.storm.testing;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.HashMap;
import java.util.Map;

public class KeyedCountingBatchBolt extends BaseBatchBolt {
    BatchOutputCollector _collector;
    Object _id;
    Map<Object, Integer> _counts = new HashMap<Object, Integer>();
    
    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
        _collector = collector;
        _id = id;
    }

    @Override
    public void execute(Tuple tuple) {
        Object key = tuple.getValue(1);
        int curr = Utils.get(_counts, key, 0);
        _counts.put(key, curr + 1);
    }

    @Override
    public void finishBatch() {
        for(Object key: _counts.keySet()) {
            _collector.emit(new Values(_id, key, _counts.get(key)));
        }
    }   

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tx", "key", "count"));
    }
    
}
