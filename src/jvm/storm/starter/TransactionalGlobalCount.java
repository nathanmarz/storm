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

public class TransactionalGlobalCount {
    public static final int PARTITION_TAKE_PER_BATCH = 3;
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
    public static final String GLOBAL_COUNT_KEY = "GLOBAL-COUNT";
        
    public static class BatchCount extends BaseBatchBolt {
        Object _id;
        BatchOutputCollector _collector;
        
        int _count = 0;

        @Override
        public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
            _collector = collector;
            _id = id;
        }

        @Override
        public void execute(Tuple tuple) {
            _count++;
        }

        @Override
        public void finishBatch() {
            _collector.emit(new Values(_id, _count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "count"));
        }        
    }
    
    public static class BatchSum extends BaseBatchBolt {
        Object _id;
        BatchOutputCollector _collector;
        
        int _sum = 0;

        @Override
        public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
            _collector = collector;
            _id = id;
        }

        @Override
        public void execute(Tuple tuple) {
            _sum+=tuple.getInteger(1);
        }

        @Override
        public void finishBatch() {
            _collector.emit(new Values(_id, _sum));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "sum"));
        }        
    }
    
    public static class CommitToDB extends BaseBasicBolt {

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            BigInteger txid = ((TransactionAttempt) tuple.getValue(0)).getTransactionId();
            int sum = tuple.getInteger(1);
            
            Value val = DATABASE.get(GLOBAL_COUNT_KEY);
            Value newval;
            if(val == null || !val.txid.equals(txid)) {
                newval = new Value();
                newval.txid = txid;
                if(val==null) {
                    newval.count = sum;
                } else {
                    newval.count = sum + val.count;
                }
                DATABASE.put(GLOBAL_COUNT_KEY, newval);
            } else {
                newval = val;
            }
            collector.emit(new Values(tuple.getValue(0), newval.count));
            
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("attempt", "global-count"));
        }
    }
    
    public static void main(String[] args) throws Exception {
        MemoryTransactionalSpout spout = new MemoryTransactionalSpout(DATA, new Fields("word"), PARTITION_TAKE_PER_BATCH);
        TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("global-count", "spout", spout, 2);
        builder.setBolt("partial-count", new BatchCount(), 5)
                .shuffleGrouping("spout");
        builder.setCommitterBolt("sum", new BatchSum())
                .globalGrouping("partial-count");
        builder.setBolt("to-db", new CommitToDB())
                .shuffleGrouping("sum");
        
        LocalCluster cluster = new LocalCluster();
        
        Config config = new Config();
        config.setDebug(true);
        config.setMaxSpoutPending(3);
        
        cluster.submitTopology("global-count-topology", config, builder.buildTopology());
        
        Thread.sleep(3000);
        cluster.shutdown();
    }
}
