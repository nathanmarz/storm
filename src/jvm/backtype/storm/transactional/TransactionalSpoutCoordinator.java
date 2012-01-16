package backtype.storm.transactional;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.transactional.state.TransactionalState;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.esotericsoftware.kryo.serialize.BigIntegerSerializer;
import java.util.HashMap;
import java.util.Map;

/**
 * TODO:
 * Need a utility for reading transactional information
 * 
 * 
 * Probably shouldn't ever repeat a transaction id... (for things where you store the txid for long periods of time, like "first time seeing an item")
 * Should use a bigint and have an efficient serializer for it
 */

public class TransactionalSpoutCoordinator implements IRichSpout {    
    public static final String TRANSACTION_BATCH_STREAM_ID = TransactionalSpoutCoordinator.class.getName() + "/batch";
    public static final String TRANSACTION_COMMIT_STREAM_ID = TransactionalSpoutCoordinator.class.getName() + "/commit";

    private static final String CURRENT_TX = "currtx";
    private static final String META_DIR = "meta";
    
    private ITransactionalSpout _spout;
    private TransactionalState _state;
    
    Map<Integer, TransactionStatus> _activeTx = new HashMap<Integer, TransactionStatus>();
    
    private SpoutOutputCollector _collector;
    int _currTransaction;
    int _maxTransactionActive;
    
    
    public TransactionalSpoutCoordinator(ITransactionalSpout spout) {
        _spout = spout;
    }
    
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _state = TransactionalState.newCoordinatorState(conf, _spout);
        _collector = collector;
        _spout.open(conf, context);
        // TODO:
        _currTransaction = getStoredCurrTransaction(_state);   
        _maxTransactionActive = Utils.getInt(conf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING));
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
        TransactionStatus status = _activeTx.get(tx.getTransactionId());
        if(!tx.equals(status.attempt)) {
            throw new IllegalStateException("Coordinator got into a bad state: acked transaction " +
                    tx.toString() + " does not match up with stored attempt: " + status);
        }
        if(status.status==AttemptStatus.PROCESSING) {
            status.status = AttemptStatus.PROCESSED;
        } else if(status.status==AttemptStatus.COMMITTING) {
            _activeTx.remove(tx.getTransactionId());
            _spout.cleanupTransaction(_currTransaction);
            _currTransaction = nextTransactionId(tx.getTransactionId());
            _state.setData(CURRENT_TX, _currTransaction);
        }
        sync();
    }

    @Override
    public void fail(Object msgId) {
        TransactionAttempt tx = (TransactionAttempt) msgId;
        TransactionStatus stored = _activeTx.remove(tx.getTransactionId());
        if(!tx.equals(stored.attempt)) {
            throw new IllegalStateException("Coordinator got into a bad state: failed transaction " +
                    tx.toString() + " does not match up with stored attempt: " + stored);
        }
        sync();
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // in partitioned example, in case an emitter task receives a later transaction than it's emitted so far,
        // when it sees the earlier txid it should know to emit nothing
        declarer.declareStream(TRANSACTION_BATCH_STREAM_ID, new Fields("tx", "tx-meta"));
        declarer.declareStream(TRANSACTION_COMMIT_STREAM_ID, new Fields("tx"));
    }
    
    private void sync() {
        TransactionStatus maybeCommit = _activeTx.get(_currTransaction);
        if(maybeCommit!=null && maybeCommit.status == AttemptStatus.PROCESSED) {
            maybeCommit.status = AttemptStatus.COMMITTING;
            _collector.emit(TRANSACTION_COMMIT_STREAM_ID, new Values(maybeCommit.attempt), maybeCommit.attempt);
        }
        
        if(_activeTx.size() < _maxTransactionActive) {
            int curr = _currTransaction;
            for(int i=0; i<_maxTransactionActive; i++) {
                if(!_activeTx.containsKey(curr)) {
                    TransactionAttempt attempt = new TransactionAttempt(curr, Utils.randomLong());
                    _activeTx.put(curr, new TransactionStatus(attempt));
                    _collector.emit(TRANSACTION_BATCH_STREAM_ID, new Values(attempt), attempt);
                }
                curr = nextTransactionId(curr);
            }
        }        
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> ret = new HashMap<String, Object>(_spout.getComponentConfiguration());
        if(!ret.containsKey(Config.TOPOLOGY_MAX_SPOUT_PENDING)) {
            ret.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        }
        ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
        return ret;
    }
    
    private static enum AttemptStatus {
        PROCESSING,
        PROCESSED,
        COMMITTING
    }
    
    private static class TransactionStatus {
        TransactionAttempt attempt;
        AttemptStatus status;
        
        public TransactionStatus(TransactionAttempt attempt) {
            this.attempt = attempt;
            this.status = AttemptStatus.PROCESSING;
        }

        @Override
        public String toString() {
            return attempt.toString() + " <" + status.toString() + ">";
        }        
    }
    
    private int nextTransactionId(int id) {
        long next = ((long) id) + 1;
        return (int) (next % Integer.MAX_VALUE);
    }
    
    private String txMetaPath(int txid) {
        return META_DIR + "/" + txid;
    }
    
    private int getStoredCurrTransaction(TransactionalState state) {
        Integer ret = (Integer) state.getData(CURRENT_TX);
        if(ret==null) return 0;
        else return ret;
    }
    
    private Object getTransactionMeta(int txid) {
        String path = txMetaPath(txid);
        Object ret = _state.getData(path);
    }
}
