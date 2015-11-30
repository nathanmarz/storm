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
package backtype.storm.spout;

import backtype.storm.Config;
import backtype.storm.state.KeyValueState;
import backtype.storm.state.StateFactory;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import static backtype.storm.spout.CheckPointState.State.COMMITTED;
import static backtype.storm.spout.CheckPointState.State.COMMITTING;
import static backtype.storm.spout.CheckPointState.State.PREPARING;

/**
 * Emits checkpoint tuples which is used to save the state of the {@link backtype.storm.topology.IStatefulComponent}
 * across the topology. If a topology contains Stateful bolts, Checkpoint spouts are automatically added
 * to the topology. There is only one Checkpoint task per topology.
 * <p/>
 * Checkpoint spout stores its internal state in a {@link KeyValueState}. The state transitions are as follows.
 * <p/>
 * <pre>
 *                  ROLLBACK(tx2)
 *               <-------------                  PREPARE(tx2)                     COMMIT(tx2)
 * COMMITTED(tx1)-------------> PREPARING(tx2) --------------> COMMITTING(tx2) -----------------> COMMITTED (tx2)
 *
 *
 * </pre>
 */
public class CheckpointSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointSpout.class);

    public static final String CHECKPOINT_STREAM_ID = "$checkpoint";
    public static final String CHECKPOINT_COMPONENT_ID = "$checkpointspout";
    public static final String CHECKPOINT_FIELD_TXID = "txid";
    public static final String CHECKPOINT_FIELD_ACTION = "action";
    public static final String CHECKPOINT_ACTION_PREPARE = "prepare";
    public static final String CHECKPOINT_ACTION_COMMIT = "commit";
    public static final String CHECKPOINT_ACTION_ROLLBACK = "rollback";
    public static final String CHECKPOINT_ACTION_INITSTATE = "initstate";

    private static final String TX_STATE_KEY = "__state";
    private static final int DEFAULT_CHECKPOINT_INTERVAL = 1000; // every sec

    private TopologyContext context;
    private SpoutOutputCollector collector;
    private long lastCheckpointTs;
    private int checkpointInterval;
    private boolean recoveryStepInProgress;
    private boolean checkpointStepInProgress;
    private boolean recovering;
    private KeyValueState<String, CheckPointState> checkpointState;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        open(context, collector, loadCheckpointInterval(conf), loadCheckpointState(conf, context));
    }

    // package access for unit test
    void open(TopologyContext context, SpoutOutputCollector collector,
              int checkpointInterval, KeyValueState<String, CheckPointState> checkpointState) {
        this.context = context;
        this.collector = collector;
        this.checkpointInterval = checkpointInterval;
        this.checkpointState = checkpointState;
        lastCheckpointTs = 0;
        recoveryStepInProgress = false;
        checkpointStepInProgress = false;
        recovering = true;
    }

    @Override
    public void nextTuple() {
        if (shouldRecover()) {
            LOG.debug("In recovery");
            handleRecovery();
            startProgress();
        } else if (shouldCheckpoint()) {
            LOG.debug("In checkpoint");
            doCheckpoint();
            startProgress();
        }
    }

    @Override
    public void ack(Object msgId) {
        CheckPointState txState = getTxState();
        LOG.debug("Got ack with txid {}, current txState {}", msgId, txState);
        if (txState.txid == ((Number) msgId).longValue()) {
            if (recovering) {
                handleRecoveryAck();
            } else {
                handleCheckpointAck();
            }
        } else {
            LOG.warn("Ack msgid {}, txState.txid {} mismatch", msgId, txState.txid);
        }
        resetProgress();
    }

    @Override
    public void fail(Object msgId) {
        LOG.debug("Got fail with msgid {}", msgId);
        resetProgress();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(CHECKPOINT_STREAM_ID, new Fields(CHECKPOINT_FIELD_TXID, CHECKPOINT_FIELD_ACTION));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_SLEEP_SPOUT_WAIT_STRATEGY_TIME_MS, 100);
        return conf;
    }

    public static boolean isCheckpoint(Tuple input) {
        return CHECKPOINT_STREAM_ID.equals(input.getSourceStreamId());
    }

    /**
     * Loads the last saved checkpoint state the from persistent storage.
     */
    private KeyValueState<String, CheckPointState> loadCheckpointState(Map conf, TopologyContext ctx) {
        String namespace = ctx.getThisComponentId() + "-" + ctx.getThisTaskId();
        KeyValueState<String, CheckPointState> state =
                (KeyValueState<String, CheckPointState>) StateFactory.getState(namespace, conf, ctx);
        if (state.get(TX_STATE_KEY) == null) {
            CheckPointState txState = new CheckPointState(-1, COMMITTED);
            state.put(TX_STATE_KEY, txState);
            state.commit();
            LOG.debug("Initialized checkpoint spout state with txState {}", txState);
        } else {
            LOG.debug("Got checkpoint spout state {}", state.get(TX_STATE_KEY));
        }
        return state;
    }

    private int loadCheckpointInterval(Map stormConf) {
        int interval;
        if (stormConf.containsKey(Config.TOPOLOGY_STATE_CHECKPOINT_INTERVAL)) {
            interval = ((Number) stormConf.get(Config.TOPOLOGY_STATE_CHECKPOINT_INTERVAL)).intValue();
        } else {
            interval = DEFAULT_CHECKPOINT_INTERVAL;
        }
        return interval;
    }

    private boolean shouldRecover() {
        return recovering && !recoveryStepInProgress;
    }

    private boolean shouldCheckpoint() {
        return !recovering && !checkpointStepInProgress
                && (System.currentTimeMillis() - lastCheckpointTs) > checkpointInterval;
    }

    private boolean shouldRollback(CheckPointState txState) {
        return txState.state == PREPARING;
    }

    private boolean shouldCommit(CheckPointState txState) {
        return txState.state == COMMITTING;
    }

    private boolean shouldInitState(CheckPointState txState) {
        return txState.state == COMMITTED;
    }

    private void handleRecovery() {
        CheckPointState txState = getTxState();
        LOG.debug("Current txState is {}", txState);
        if (shouldRollback(txState)) {
            LOG.debug("Emitting rollback with txid {}", txState.txid);
            collector.emit(CHECKPOINT_STREAM_ID, new Values(txState.txid, CHECKPOINT_ACTION_ROLLBACK), txState.txid);
        } else if (shouldCommit(txState)) {
            LOG.debug("Emitting commit with txid {}", txState.txid);
            collector.emit(CHECKPOINT_STREAM_ID, new Values(txState.txid, CHECKPOINT_ACTION_COMMIT), txState.txid);
        } else if (shouldInitState(txState)) {
            LOG.debug("Emitting init state with txid {}", txState.txid);
            collector.emit(CHECKPOINT_STREAM_ID, new Values(txState.txid, CHECKPOINT_ACTION_INITSTATE), txState.txid);
        }
        startProgress();
    }

    private void handleRecoveryAck() {
        CheckPointState txState = getTxState();
        if (shouldRollback(txState)) {
            txState.state = COMMITTED;
            --txState.txid;
            saveTxState(txState);
        } else if (shouldCommit(txState)) {
            txState.state = COMMITTED;
            saveTxState(txState);
        } else if (shouldInitState(txState)) {
            LOG.debug("Recovery complete, current state {}", txState);
            recovering = false;
        }
    }

    private void doCheckpoint() {
        CheckPointState txState = getTxState();
        if (txState.state == COMMITTED) {
            txState.txid++;
            txState.state = PREPARING;
            saveTxState(txState);
            lastCheckpointTs = System.currentTimeMillis();
            LOG.debug("Emitting prepare with txid {}", txState.txid);
            collector.emit(CHECKPOINT_STREAM_ID, new Values(txState.txid, CHECKPOINT_ACTION_PREPARE), txState.txid);
        } else if (txState.state == PREPARING) {
            LOG.debug("Emitting prepare with txid {}", txState.txid);
            collector.emit(CHECKPOINT_STREAM_ID, new Values(txState.txid, CHECKPOINT_ACTION_PREPARE), txState.txid);
        } else if (txState.state == COMMITTING) {
            LOG.debug("Emitting commit with txid {}", txState.txid);
            collector.emit(CHECKPOINT_STREAM_ID, new Values(txState.txid, CHECKPOINT_ACTION_COMMIT), txState.txid);
        }
        startProgress();
    }

    private void handleCheckpointAck() {
        CheckPointState txState = getTxState();
        if (txState.state == PREPARING) {
            txState.state = COMMITTING;
            LOG.debug("Prepare txid {} complete", txState.txid);
        } else if (txState.state == COMMITTING) {
            txState.state = COMMITTED;
            LOG.debug("Commit txid {} complete", txState.txid);
        }
        saveTxState(txState);
    }

    private void saveTxState(CheckPointState txState) {
        checkpointState.put(TX_STATE_KEY, txState);
        checkpointState.commit();
    }

    private CheckPointState getTxState() {
        return checkpointState.get(TX_STATE_KEY);
    }

    private void startProgress() {
        if (recovering) {
            recoveryStepInProgress = true;
        } else {
            checkpointStepInProgress = true;
        }
    }

    private void resetProgress() {
        if (recovering) {
            recoveryStepInProgress = false;
        } else {
            checkpointStepInProgress = false;
        }
    }
}
