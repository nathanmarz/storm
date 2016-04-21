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
package org.apache.storm.topology;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.spout.CheckPointState;
import org.apache.storm.spout.CheckpointSpout;
import org.apache.storm.task.IOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.storm.spout.CheckPointState.Action.ROLLBACK;
import static org.apache.storm.spout.CheckpointSpout.CHECKPOINT_FIELD_ACTION;
import static org.apache.storm.spout.CheckpointSpout.CHECKPOINT_FIELD_TXID;
import static org.apache.storm.spout.CheckpointSpout.CHECKPOINT_STREAM_ID;

/**
 * Base class that abstracts the common logic for executing bolts in a stateful topology.
 */
public abstract class BaseStatefulBoltExecutor implements IRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(BaseStatefulBoltExecutor.class);
    private final Map<TransactionRequest, Integer> transactionRequestCount;
    private int checkPointInputTaskCount;
    private long lastTxid = Long.MIN_VALUE;
    protected OutputCollector collector;

    public BaseStatefulBoltExecutor() {
        transactionRequestCount = new HashMap<>();
    }

    protected void init(TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.checkPointInputTaskCount = getCheckpointInputTaskCount(context);
    }

    /**
     * returns the total number of input checkpoint streams across
     * all input tasks to this component.
     */
    private int getCheckpointInputTaskCount(TopologyContext context) {
        int count = 0;
        for (GlobalStreamId inputStream : context.getThisSources().keySet()) {
            if (CHECKPOINT_STREAM_ID.equals(inputStream.get_streamId())) {
                count += context.getComponentTasks(inputStream.get_componentId()).size();
            }
        }
        return count;
    }

    @Override
    public void execute(Tuple input) {
        if (CheckpointSpout.isCheckpoint(input)) {
            processCheckpoint(input);
        } else {
            handleTuple(input);
        }
    }

    /**
     * Invokes handleCheckpoint once checkpoint tuple is received on
     * all input checkpoint streams to this component.
     */
    private void processCheckpoint(Tuple input) {
        CheckPointState.Action action = (CheckPointState.Action) input.getValueByField(CHECKPOINT_FIELD_ACTION);
        long txid = input.getLongByField(CHECKPOINT_FIELD_TXID);
        if (shouldProcessTransaction(action, txid)) {
            LOG.debug("Processing action {}, txid {}", action, txid);
            try {
                if (txid >= lastTxid) {
                    handleCheckpoint(input, action, txid);
                    if (action == ROLLBACK) {
                        lastTxid = txid - 1;
                    } else {
                        lastTxid = txid;
                    }
                } else {
                    LOG.debug("Ignoring old transaction. Action {}, txid {}", action, txid);
                    collector.ack(input);
                }
            } catch (Throwable th) {
                LOG.error("Got error while processing checkpoint tuple", th);
                collector.fail(input);
                collector.reportError(th);
            }
        } else {
            LOG.debug("Waiting for action {}, txid {} from all input tasks. checkPointInputTaskCount {}, " +
                              "transactionRequestCount {}", action, txid, checkPointInputTaskCount, transactionRequestCount);
            collector.ack(input);
        }
    }

    /**
     * Checks if check points have been received from all tasks across
     * all input streams to this component
     */
    private boolean shouldProcessTransaction(CheckPointState.Action action, long txid) {
        TransactionRequest request = new TransactionRequest(action, txid);
        Integer count;
        if ((count = transactionRequestCount.get(request)) == null) {
            transactionRequestCount.put(request, 1);
            count = 1;
        } else {
            transactionRequestCount.put(request, ++count);
        }
        if (count == checkPointInputTaskCount) {
            transactionRequestCount.remove(request);
            return true;
        }
        return false;
    }

    protected void declareCheckpointStream(OutputFieldsDeclarer declarer) {
        declarer.declareStream(CHECKPOINT_STREAM_ID, new Fields(CHECKPOINT_FIELD_TXID, CHECKPOINT_FIELD_ACTION));
    }

    /**
     * Sub-classes can implement the logic for handling the tuple.
     *
     * @param input the input tuple
     */
    protected abstract void handleTuple(Tuple input);

    /**
     * Sub-classes can implement the logic for handling checkpoint tuple.
     *
     * @param checkpointTuple the checkpoint tuple
     * @param action          the action (prepare, commit, rollback or initstate)
     * @param txid            the transaction id.
     */
    protected abstract void handleCheckpoint(Tuple checkpointTuple, CheckPointState.Action action, long txid);

    protected static class AnchoringOutputCollector extends OutputCollector {
        AnchoringOutputCollector(IOutputCollector delegate) {
            super(delegate);
        }

        @Override
        public List<Integer> emit(String streamId, List<Object> tuple) {
            throw new UnsupportedOperationException("Bolts in a stateful topology must emit anchored tuples.");
        }

        @Override
        public void emitDirect(int taskId, String streamId, List<Object> tuple) {
            throw new UnsupportedOperationException("Bolts in a stateful topology must emit anchored tuples.");
        }
    }

    private static class TransactionRequest {
        private final CheckPointState.Action action;

        private final long txid;

        TransactionRequest(CheckPointState.Action action, long txid) {
            this.action = action;
            this.txid = txid;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TransactionRequest that = (TransactionRequest) o;

            if (txid != that.txid) return false;
            return !(action != null ? !action.equals(that.action) : that.action != null);

        }

        @Override
        public int hashCode() {
            int result = action != null ? action.hashCode() : 0;
            result = 31 * result + (int) (txid ^ (txid >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return "TransactionRequest{" +
                    "action='" + action + '\'' +
                    ", txid=" + txid +
                    '}';
        }

    }
}
