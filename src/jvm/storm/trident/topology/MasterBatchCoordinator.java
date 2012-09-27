package storm.trident.topology;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import backtype.storm.utils.WindowedTimeThrottler;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Random;
import org.apache.log4j.Logger;
import storm.trident.spout.ITridentSpout;
import storm.trident.topology.state.TransactionalState;

public class MasterBatchCoordinator extends BaseRichSpout { 
    public static final Logger LOG = Logger.getLogger(MasterBatchCoordinator.class);
    
    public static final long INIT_TXID = 1L;
    
    
    public static final String BATCH_STREAM_ID = "$batch";
    public static final String COMMIT_STREAM_ID = "$commit";
    public static final String SUCCESS_STREAM_ID = "$success";

    private static final String CURRENT_TX = "currtx";
    
    private List<TransactionalState> _states = new ArrayList();
    
    TreeMap<Long, TransactionStatus> _activeTx = new TreeMap<Long, TransactionStatus>();
    
    private SpoutOutputCollector _collector;
    private Random _rand;
    Long _currTransaction;
    int _maxTransactionActive;
    
    List<ITridentSpout.BatchCoordinator> _coordinators = new ArrayList();
    
    
    List<String> _managedSpoutIds;
    List<ITridentSpout> _spouts;
    WindowedTimeThrottler _throttler;
    
    boolean _active = true;
    
    public MasterBatchCoordinator(List<String> spoutIds, List<ITridentSpout> spouts) {
        if(spoutIds.isEmpty()) {
            throw new IllegalArgumentException("Must manage at least one spout");
        }
        _managedSpoutIds = spoutIds;
        _spouts = spouts;
    }

    @Override
    public void activate() {
        _active = true;
    }

    @Override
    public void deactivate() {
        _active = false;
    }
        
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _rand = new Random(Utils.secureRandomLong());
        _throttler = new WindowedTimeThrottler((Number)conf.get(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS), 1);
        for(String spoutId: _managedSpoutIds) {
            _states.add(TransactionalState.newCoordinatorState(conf, spoutId));
        }
        _currTransaction = getStoredCurrTransaction();

        _collector = collector;
        Number active = (Number) conf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING);
        if(active==null) {
            _maxTransactionActive = 1;
        } else {
            _maxTransactionActive = active.intValue();
        }
        
        for(int i=0; i<_spouts.size(); i++) {
            String txId = _managedSpoutIds.get(i);
            _coordinators.add(_spouts.get(i).getCoordinator(txId, conf, context));
        }
    }

    @Override
    public void close() {
        for(TransactionalState state: _states) {
            state.close();
        }
    }

    @Override
    public void nextTuple() {
        sync();
    }

    @Override
    public void ack(Object msgId) {
        TransactionAttempt tx = (TransactionAttempt) msgId;
        TransactionStatus status = _activeTx.get(tx.getTransactionId());
        if(status!=null && tx.equals(status.attempt)) {
            if(status.status==AttemptStatus.PROCESSING) {
                status.status = AttemptStatus.PROCESSED;
            } else if(status.status==AttemptStatus.COMMITTING) {
                _activeTx.remove(tx.getTransactionId());
                _collector.emit(SUCCESS_STREAM_ID, new Values(tx));
                _currTransaction = nextTransactionId(tx.getTransactionId());
                for(TransactionalState state: _states) {
                    state.setData(CURRENT_TX, _currTransaction);                    
                }
            }
            sync();
        }
    }

    @Override
    public void fail(Object msgId) {
        TransactionAttempt tx = (TransactionAttempt) msgId;
        TransactionStatus stored = _activeTx.remove(tx.getTransactionId());
        if(stored!=null && tx.equals(stored.attempt)) {
            _activeTx.tailMap(tx.getTransactionId()).clear();
            sync();
        }
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // in partitioned example, in case an emitter task receives a later transaction than it's emitted so far,
        // when it sees the earlier txid it should know to emit nothing
        declarer.declareStream(BATCH_STREAM_ID, new Fields("tx"));
        declarer.declareStream(COMMIT_STREAM_ID, new Fields("tx"));
        declarer.declareStream(SUCCESS_STREAM_ID, new Fields("tx"));
    }
    
    private void sync() {
        // note that sometimes the tuples active may be less than max_spout_pending, e.g.
        // max_spout_pending = 3
        // tx 1, 2, 3 active, tx 2 is acked. there won't be a commit for tx 2 (because tx 1 isn't committed yet),
        // and there won't be a batch for tx 4 because there's max_spout_pending tx active
        TransactionStatus maybeCommit = _activeTx.get(_currTransaction);
        if(maybeCommit!=null && maybeCommit.status == AttemptStatus.PROCESSED) {
            maybeCommit.status = AttemptStatus.COMMITTING;
            _collector.emit(COMMIT_STREAM_ID, new Values(maybeCommit.attempt), maybeCommit.attempt);
        }
        
        if(_active) {
            if(_activeTx.size() < _maxTransactionActive) {
                Long curr = _currTransaction;
                for(int i=0; i<_maxTransactionActive; i++) {
                    if(!_activeTx.containsKey(curr) && isReady(curr)) {
                      
                        TransactionAttempt attempt = new TransactionAttempt(curr, _rand.nextLong());
                        _activeTx.put(curr, new TransactionStatus(attempt));
                        _collector.emit(BATCH_STREAM_ID, new Values(attempt), attempt);
                        _throttler.markEvent();
                    }
                    curr = nextTransactionId(curr);
                }
            }
        }
    }
    
    private boolean isReady(long txid) {
        if(_throttler.isThrottled()) return false;
        //TODO: make this strategy configurable?... right now it goes if anyone is ready
        for(ITridentSpout.BatchCoordinator coord: _coordinators) {
            if(coord.isReady(txid)) return true;
        }
        return false;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
        ret.registerSerialization(TransactionAttempt.class);
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
    
    
    private Long nextTransactionId(Long id) {
        return id + 1;
    }  
    
    private Long getStoredCurrTransaction() {
        Long ret = INIT_TXID;
        for(TransactionalState state: _states) {
            Long curr = (Long) state.getData(CURRENT_TX);
            if(curr!=null && curr.compareTo(ret) > 0) {
                ret = curr;
            }
        }
        return ret;
    }
}
