/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.trident.topology;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.WindowedTimeThrottler;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.spout.ITridentSpout;
import storm.trident.topology.state.TransactionalState;

public class MasterBatchCoordinator extends BaseRichSpout { 
    public static final Logger LOG = LoggerFactory.getLogger(MasterBatchCoordinator.class);
    
    public static final long INIT_TXID = 1L;
    
    
    public static final String BATCH_STREAM_ID = "$batch";
    public static final String COMMIT_STREAM_ID = "$commit";
    public static final String SUCCESS_STREAM_ID = "$success";

    private static final String CURRENT_TX = "currtx";
    private static final String CURRENT_ATTEMPTS = "currattempts";
    
    private List<TransactionalState> _states = new ArrayList();
    
    TreeMap<Long, TransactionStatus> _activeTx = new TreeMap<Long, TransactionStatus>();
    TreeMap<Long, Integer> _attemptIds;
    
    private SpoutOutputCollector _collector;
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

    public List<String> getManagedSpoutIds(){
        return _managedSpoutIds;
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
        _attemptIds = getStoredCurrAttempts(_currTransaction, _maxTransactionActive);

        
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
                _attemptIds.remove(tx.getTransactionId());
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
                        // by using a monotonically increasing attempt id, downstream tasks
                        // can be memory efficient by clearing out state for old attempts
                        // as soon as they see a higher attempt id for a transaction
                        Integer attemptId = _attemptIds.get(curr);
                        if(attemptId==null) {
                            attemptId = 0;
                        } else {
                            attemptId++;
                        }
                        _attemptIds.put(curr, attemptId);
                        for(TransactionalState state: _states) {
                            state.setData(CURRENT_ATTEMPTS, _attemptIds);
                        }
                        
                        TransactionAttempt attempt = new TransactionAttempt(curr, attemptId);
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
    
    private TreeMap<Long, Integer> getStoredCurrAttempts(long currTransaction, int maxBatches) {
        TreeMap<Long, Integer> ret = new TreeMap<Long, Integer>();
        for(TransactionalState state: _states) {
            Map<Object, Number> attempts = (Map) state.getData(CURRENT_ATTEMPTS);
            if(attempts==null) attempts = new HashMap();
            for(Entry<Object, Number> e: attempts.entrySet()) {
                // this is because json doesn't allow numbers as keys...
                // TODO: replace json with a better form of encoding
                Number txidObj;
                if(e.getKey() instanceof String) {
                    txidObj = Long.parseLong((String) e.getKey());
                } else {
                    txidObj = (Number) e.getKey();
                }
                long txid = ((Number) txidObj).longValue();
                int attemptId = ((Number) e.getValue()).intValue();
                Integer curr = ret.get(txid);
                if(curr==null || attemptId > curr) {
                    ret.put(txid, attemptId);
                }                
            }
        }
        ret.headMap(currTransaction).clear();
        ret.tailMap(currTransaction + maxBatches - 1).clear();
        return ret;
    }
}
