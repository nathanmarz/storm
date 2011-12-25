package backtype.storm.transactional;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

// TODO: need a way to override max spout pending with max batc pending conf
public class TransactionalSpoutCoordinator implements IRichSpout {
    public static final String TRANSACTION_BATCH_STREAM_ID = TransactionalSpoutCoordinator.class.getName() + "/batch";
    public static final String TRANSACTION_COMMIT_STREAM_ID = TransactionalSpoutCoordinator.class.getName() + "/commit";

    private ITransactionalSpout _spout;
    private ITransactionState _state;
    
    TreeMap<TransactionAttempt, AttemptStatus> _activeTx = new TreeMap<TransactionAttempt, AttemptStatus>();
    
    private SpoutOutputCollector _collector;
    int _currTransaction;
    int _maxTransactionActive;
    
    
    public TransactionalSpoutCoordinator(ITransactionalSpout spout) {
        _spout = spout;
    }
    
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _state = _spout.getState();
        _state.open(conf, context);
        _collector = collector;
        _currTransaction = _state.getTransactionId();
        _maxTransactionActive = Utils.getInt(conf.get(Config.TOPOLOGY_MAX_TRANSACTION_ACTIVE));
    }

    @Override
    public void close() {
        _state.close();
    }

    @Override
    public void nextTuple() {
        sync();
    }

    @Override
    public void ack(Object msgId) {
        TransactionAttempt tx = (TransactionAttempt) msgId;
        AttemptStatus status = _activeTx.get(tx);
        if(status==AttemptStatus.PROCESSING) {
            _activeTx.put(tx, AttemptStatus.PROCESSED);
        } else if(status==AttemptStatus.COMMITTING) {
            _activeTx.remove(tx);
            _currTransaction = nextTransactionId(tx.getTransactionId());
            _state.setTransactionId(_currTransaction);
        }
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
        TransactionAttempt first = _activeTx.firstKey();
        if(_activeTx.get(first)==AttemptStatus.PROCESSED &&
            first.getTransactionId()==_currTransaction) {
            _activeTx.put(first, AttemptStatus.COMMITTING);
            _collector.emit(TRANSACTION_COMMIT_STREAM_ID, new Values(first), first);
        }
        
        if(_activeTx.size() < _maxTransactionActive) {
            Set<Integer> activeTxid = new HashSet<Integer>();
            for(TransactionAttempt attempt: _activeTx.keySet()) {
                activeTxid.add(attempt.getTransactionId());
            }
            int curr = _currTransaction;
            for(int i=0; i<_maxTransactionActive; i++) {
                if(!activeTxid.contains(curr)) {
                    TransactionAttempt attempt = new TransactionAttempt(curr, Utils.randomLong());
                    _activeTx.put(attempt, AttemptStatus.PROCESSING);
                    _collector.emit(TRANSACTION_BATCH_STREAM_ID, new Values(attempt), attempt);
                }
                curr = nextTransactionId(curr);
            }
        }        
    }
    
    private static enum AttemptStatus {
        PROCESSING,
        PROCESSED,
        COMMITTING
    }
    
    private int nextTransactionId(int id) {
        long next = ((long) id) + 1;
        return (int) (next % Integer.MAX_VALUE);
    }
}
