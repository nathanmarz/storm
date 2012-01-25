package backtype.storm.testing;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.topology.base.BaseCommitterBolt;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.HashMap;
import java.util.Map;

public class KeyedCountingCommitterBolt extends BaseCommitterBolt {
    TransactionAttempt _id;
    Map<Object, Integer> _counts = new HashMap<Object, Integer>();
    
    @Override
    public void prepare(Map conf, TopologyContext context, TransactionAttempt id) {
        _id = id;
    }

    @Override
    public void execute(Tuple tuple) {
        Object key = tuple.getValue(1);
        int curr = Utils.get(_counts, key, 0);
        _counts.put(key, curr + 1);
    }

    @Override
    public void commit(BatchOutputCollector collector) {
        for(Object key: _counts.keySet()) {
            collector.emit(new Values(_id, key, _counts.get(key)));
        }
    }   

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tx", "key", "count"));
    }
    
}
