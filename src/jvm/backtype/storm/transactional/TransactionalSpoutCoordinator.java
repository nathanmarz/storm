package backtype.storm.transactional;

import backtype.storm.coordination.FailedBatchException;
import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.transactional.state.RotatingTransactionalState;
import backtype.storm.transactional.state.TransactionalState;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;

public class TransactionalSpoutCoordinator implements IRichSpout { 
    public static final Logger LOG = Logger.getLogger(TransactionalSpoutCoordinator.class);
    
    public static final BigInteger INIT_TXID = new BigInteger("1");
    
    
    public static final String TRANSACTION_BATCH_STREAM_ID = TransactionalSpoutCoordinator.class.getName() + "/batch";
    public static final String TRANSACTION_COMMIT_STREAM_ID = TransactionalSpoutCoordinator.class.getName() + "/commit";

    private static final String CURRENT_TX = "currtx";
    private static final String META_DIR = "meta";
    
    private ITransactionalSpout _spout;
    private ITransactionalSpout.Coordinator _coordinator;
    private TransactionalState _state;
    private RotatingTransactionalState _coordinatorState;
    
    Map<BigInteger, TransactionStatus> _activeTx = new HashMap<BigInteger, TransactionStatus>();
    
    private SpoutOutputCollector _collector;
    BigInteger _currTransaction;
    int _maxTransactionActive;
    StateInitializer _initializer;
    
    
    public TransactionalSpoutCoordinator(ITransactionalSpout spout) {
        _spout = spout;
    }
    
    public ITransactionalSpout getSpout() {
        return _spout;
    }
    
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _state = TransactionalState.newCoordinatorState(conf, (String) conf.get(Config.TOPOLOGY_TRANSACTIONAL_ID), _spout.getComponentConfiguration());
        _coordinatorState = new RotatingTransactionalState(_state, META_DIR, true);
        _collector = collector;
        _coordinator = _spout.getCoordinator(conf, context);
        _currTransaction = getStoredCurrTransaction(_state);   
        _maxTransactionActive = Utils.getInt(conf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING));
        _initializer = new StateInitializer();
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
            _coordinatorState.cleanupBefore(tx.getTransactionId());
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
        declarer.declareStream(TRANSACTION_BATCH_STREAM_ID, new Fields("tx", "tx-meta", "curr-txid"));
        declarer.declareStream(TRANSACTION_COMMIT_STREAM_ID, new Fields("tx"));
    }
    
    private void sync() {
        // TODO: this code might be redundant. can just find the next transaction that needs a batch or commit tuple
        // and emit that, instead of iterating through (MAX_SPOUT_PENDING should take care of things)
        TransactionStatus maybeCommit = _activeTx.get(_currTransaction);
        if(maybeCommit!=null && maybeCommit.status == AttemptStatus.PROCESSED) {
            maybeCommit.status = AttemptStatus.COMMITTING;
            _collector.emit(TRANSACTION_COMMIT_STREAM_ID, new Values(maybeCommit.attempt), maybeCommit.attempt);
        }
        
        try {
            if(_activeTx.size() < _maxTransactionActive) {
                BigInteger curr = _currTransaction;
                for(int i=0; i<_maxTransactionActive; i++) {
                    if(!_activeTx.containsKey(curr)) {
                        TransactionAttempt attempt = new TransactionAttempt(curr, Utils.randomLong());
                        Object state = _coordinatorState.getState(curr, _initializer);
                        _activeTx.put(curr, new TransactionStatus(attempt));
                        _collector.emit(TRANSACTION_BATCH_STREAM_ID, new Values(attempt, state, _currTransaction), attempt);
                    }
                    curr = nextTransactionId(curr);
                }
            }     
        } catch(FailedBatchException e) {
            LOG.warn("Failed to get metadata for a transaction", e);
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
    
    
    private static final BigInteger ONE = new BigInteger("1");
    private BigInteger nextTransactionId(BigInteger id) {
        return id.add(ONE);
    }
    
    private BigInteger getStoredCurrTransaction(TransactionalState state) {
        BigInteger ret = (BigInteger) state.getData(CURRENT_TX);
        if(ret==null) return ONE;
        else return ret;
    }
    
    private class StateInitializer implements RotatingTransactionalState.StateInitializer {
        @Override
        public Object init(BigInteger txid, Object lastState) {
            return _coordinator.initializeTransaction(txid, lastState);
        }
    }
}
