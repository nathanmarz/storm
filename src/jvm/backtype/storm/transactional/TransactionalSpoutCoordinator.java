package backtype.storm.transactional;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public class TransactionalSpoutCoordinator implements IRichSpout {
    public static final String TRANSACTION_BATCH_STREAM_ID = TransactionalSpoutCoordinator.class.getName() + "/batch";
    public static final String TRANSACTION_COMMIT_STREAM_ID = TransactionalSpoutCoordinator.class.getName() + "/commit";

    private ITransactionalSpout _spout;
    private ITransactionState _state;
    
    private Set<TransactionAttempt> _activeTx = new HashSet<TransactionAttempt>();
    private SortedSet<Integer> _completedTx = new TreeSet<Integer>();
    private SpoutOutputCollector _collector;
    
    public TransactionalSpoutCoordinator(ITransactionalSpout spout) {
        _spout = spout;
    }
    
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _state = _spout.getState();
        _state.open(conf, context);
        _collector = collector;
    }

    @Override
    public void close() {
        _state.close();
    }

    @Override
    public void nextTuple() {
        // TODO: implement TOPOLOGY-MAX-BATCH-PENDING?? this conflicts with max spout pending...
        // need a way to override default confs?
        sync();
    }

    @Override
    public void ack(Object msgId) {
        // TODO: this is wrong... need to distinguish between committing and batching
        TransactionAttempt tx = (TransactionAttempt) msgId;
        _completedTx.add(tx.getTransactionId());
        _activeTx.remove(tx);
        sync();
    }

    @Override
    public void fail(Object msgId) {
        TransactionAttempt tx = (TransactionAttempt) msgId;
        _activeTx.remove(tx);
        sync();
    }

    @Override
    public boolean isDistributed() {
        return false;
    }    
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(TRANSACTION_BATCH_STREAM_ID, new Fields("tx"));
        declarer.declareStream(TRANSACTION_COMMIT_STREAM_ID, new Fields("tx"));
    }
    
    private void sync() {
        // go through completedTx and officially commit them to state if its time
        // add next missing transaction (whether redo or pipelined)
    }
}
