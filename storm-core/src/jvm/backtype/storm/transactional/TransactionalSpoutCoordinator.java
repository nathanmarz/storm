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
package backtype.storm.transactional;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.transactional.state.RotatingTransactionalState;
import backtype.storm.transactional.state.TransactionalState;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.math.BigInteger;
import java.util.Map;
import java.util.TreeMap;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionalSpoutCoordinator extends BaseRichSpout { 
    public static final Logger LOG = LoggerFactory.getLogger(TransactionalSpoutCoordinator.class);
    
    public static final BigInteger INIT_TXID = BigInteger.ONE;
    
    
    public static final String TRANSACTION_BATCH_STREAM_ID = TransactionalSpoutCoordinator.class.getName() + "/batch";
    public static final String TRANSACTION_COMMIT_STREAM_ID = TransactionalSpoutCoordinator.class.getName() + "/commit";

    private static final String CURRENT_TX = "currtx";
    private static final String META_DIR = "meta";
    
    private ITransactionalSpout _spout;
    private ITransactionalSpout.Coordinator _coordinator;
    private TransactionalState _state;
    private RotatingTransactionalState _coordinatorState;
    
    TreeMap<BigInteger, TransactionStatus> _activeTx = new TreeMap<BigInteger, TransactionStatus>();
    
    private SpoutOutputCollector _collector;
    private Random _rand;
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
        _rand = new Random(Utils.secureRandomLong());
        _state = TransactionalState.newCoordinatorState(conf, (String) conf.get(Config.TOPOLOGY_TRANSACTIONAL_ID), _spout.getComponentConfiguration());
        _coordinatorState = new RotatingTransactionalState(_state, META_DIR, true);
        _collector = collector;
        _coordinator = _spout.getCoordinator(conf, context);
        _currTransaction = getStoredCurrTransaction(_state);
        Object active = conf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING);
        if(active==null) {
            _maxTransactionActive = 1;
        } else {
            _maxTransactionActive = Utils.getInt(active);
        }
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
        if(status!=null && tx.equals(status.attempt)) {
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
        declarer.declareStream(TRANSACTION_BATCH_STREAM_ID, new Fields("tx", "tx-meta", "committed-txid"));
        declarer.declareStream(TRANSACTION_COMMIT_STREAM_ID, new Fields("tx"));
    }
    
    private void sync() {
        // note that sometimes the tuples active may be less than max_spout_pending, e.g.
        // max_spout_pending = 3
        // tx 1, 2, 3 active, tx 2 is acked. there won't be a commit for tx 2 (because tx 1 isn't committed yet),
        // and there won't be a batch for tx 4 because there's max_spout_pending tx active
        TransactionStatus maybeCommit = _activeTx.get(_currTransaction);
        if(maybeCommit!=null && maybeCommit.status == AttemptStatus.PROCESSED) {
            maybeCommit.status = AttemptStatus.COMMITTING;
            _collector.emit(TRANSACTION_COMMIT_STREAM_ID, new Values(maybeCommit.attempt), maybeCommit.attempt);
        }
        
        try {
            if(_activeTx.size() < _maxTransactionActive) {
                BigInteger curr = _currTransaction;
                for(int i=0; i<_maxTransactionActive; i++) {
                    if((_coordinatorState.hasCache(curr) || _coordinator.isReady())
                            && !_activeTx.containsKey(curr)) {
                        TransactionAttempt attempt = new TransactionAttempt(curr, _rand.nextLong());
                        Object state = _coordinatorState.getState(curr, _initializer);
                        _activeTx.put(curr, new TransactionStatus(attempt));
                        _collector.emit(TRANSACTION_BATCH_STREAM_ID, new Values(attempt, state, previousTransactionId(_currTransaction)), attempt);
                    }
                    curr = nextTransactionId(curr);
                }
            }     
        } catch(FailedException e) {
            LOG.warn("Failed to get metadata for a transaction", e);
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
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
    
    
    private BigInteger nextTransactionId(BigInteger id) {
        return id.add(BigInteger.ONE);
    }
    
    private BigInteger previousTransactionId(BigInteger id) {
        if(id.equals(INIT_TXID)) {
            return null;
        } else {
            return id.subtract(BigInteger.ONE);
        }
    }    
    
    private BigInteger getStoredCurrTransaction(TransactionalState state) {
        BigInteger ret = (BigInteger) state.getData(CURRENT_TX);
        if(ret==null) return INIT_TXID;
        else return ret;
    }
    
    private class StateInitializer implements RotatingTransactionalState.StateInitializer {
        @Override
        public Object init(BigInteger txid, Object lastState) {
            return _coordinator.initializeTransaction(txid, lastState);
        }
    }
}
