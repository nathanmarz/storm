package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.MemoryTransactionalSpout;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.ICommitter;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.transactional.TransactionalTopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransactionalTopWords {
    public static final int PARTITION_TAKE_PER_BATCH = 3;
    public static final int TOP_AMOUNT = 10;
    
    public static final Map<Integer, List<List<Object>>> DATA = new HashMap<Integer, List<List<Object>>>() {{
        put(0, new ArrayList<List<Object>>() {{
            add(new Values("cat"));
            add(new Values("dog"));
            add(new Values("chicken"));
            add(new Values("cat"));
            add(new Values("dog"));
            add(new Values("apple"));
        }});
        put(1, new ArrayList<List<Object>>() {{
            add(new Values("cat"));
            add(new Values("dog"));
            add(new Values("apple"));
            add(new Values("banana"));
        }});
        put(2, new ArrayList<List<Object>>() {{
            add(new Values("cat"));
            add(new Values("cat"));
            add(new Values("cat"));
            add(new Values("cat"));
            add(new Values("cat"));
            add(new Values("dog"));
            add(new Values("dog"));
            add(new Values("dog"));
            add(new Values("dog"));
        }});
    }};
    
    public static class Value {
        int count = 0;
        BigInteger txid;
    }

    public static Map<String, Value> DATABASE = new HashMap<String, Value>();
    public static String TOP_KEY = "__TOP";
        
    public static class KeyedCountUpdater extends BaseTransactionalBolt implements ICommitter {
        Map<String, Integer> _counts = new HashMap<String, Integer>();
        BatchOutputCollector _collector;
        TransactionAttempt _id;
        
        int _count = 0;

        @Override
        public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, TransactionAttempt id) {
            _collector = collector;
            _id = id;
        }

        @Override
        public void execute(Tuple tuple) {
            String key = tuple.getString(0);
            Integer curr = _counts.get(key);
            if(curr==null) curr = 0;
            _counts.put(key, curr + 1);
        }

        @Override
        public void finishBatch() {
            for(String key: _counts.keySet()) {
                Value val = DATABASE.get(key);
                if(!val.txid.equals(_id)) {
                    val.txid = _id.getTransactionId();
                    val.count = val.count + _counts.get(key);
                    DATABASE.put(key, val);
                }
                _collector.emit(new Values(_id, key, val.count));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "key", "count"));
        }        
    }
    
    public static class RankN extends BaseBatchBolt {
        BatchOutputCollector _collector;
        Object _id;
        int _n;
        
        public RankN(int n) {
            _n = n;
        }
        
        @Override
        public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
            _collector = collector;
            _id = id;
        }

        @Override
        public void execute(Tuple tuple) {
            // TODO: finish
        }

        @Override
        public void finishBatch() {
            // TODO: finish
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "list"));
        }
    }
    
    public static class MergeN extends BaseBatchBolt {
        BatchOutputCollector _collector;
        Object _id;
        int _n;
        
        public MergeN(int n) {
            _n = n;
        }
        
        @Override
        public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
            _collector = collector;
            _id = id;
        }

        @Override
        public void execute(Tuple tuple) {
            // TODO: finish
        }

        @Override
        public void finishBatch() {
            // TODO: finish
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "list"));
        }
    }
    
    public static class WriteTopN extends BaseBasicBolt {

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            // TODO: get and merge with the list from the database
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }        
    }
    
    public static void main(String[] args) throws Exception {
        MemoryTransactionalSpout spout = new MemoryTransactionalSpout(DATA, new Fields("word"), PARTITION_TAKE_PER_BATCH);
        TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("top-n-words", "spout", spout, 2);
        builder.setBolt("count", new KeyedCountUpdater(), 5)
                .fieldsGrouping("spout", new Fields("word"));
        builder.setBolt("rank", new RankN(TOP_AMOUNT), 5)
                .shuffleGrouping("count");
        builder.setBolt("merge", new MergeN(TOP_AMOUNT))
                .globalGrouping("rank");
        builder.setBolt("write-top-n", new WriteTopN())
                .shuffleGrouping("merge");
        
        LocalCluster cluster = new LocalCluster();
        
        Config config = new Config();
        config.setDebug(true);
        config.setMaxSpoutPending(3);
        
        cluster.submitTopology("top-n-topology", config, builder.buildTopology());
        
        Thread.sleep(3000);
        cluster.shutdown();
    }
}
