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
package org.apache.storm.spout;

import static org.apache.storm.spout.CheckPointState.State.COMMITTED;
import static org.apache.storm.spout.CheckPointState.State.COMMITTING;
import static org.apache.storm.spout.CheckPointState.State.PREPARING;

/**
 * Captures the current state of the transaction in {@link CheckpointSpout}. The state transitions are as follows.
 * <pre>
 *                  ROLLBACK(tx2)
 *               <-------------                  PREPARE(tx2)                     COMMIT(tx2)
 * COMMITTED(tx1)-------------> PREPARING(tx2) --------------> COMMITTING(tx2) -----------------> COMMITTED (tx2)
 *
 * </pre>
 *
 * During recovery, if a previous transaction is in PREPARING state, it is rolled back since all bolts in the topology
 * might not have prepared (saved) the data for commit. If the previous transaction is in COMMITTING state, it is
 * rolled forward (committed) since some bolts might have already committed the data.
 * <p>
 * During normal flow, the state transitions from PREPARING to COMMITTING to COMMITTED. In case of failures the
 * prepare/commit operation is retried.
 * </p>
 */
public class CheckPointState {
    private long txid;
    private State state;

    public enum State {
        /**
         * The checkpoint spout has committed the transaction.
         */
        COMMITTED,
        /**
         * The checkpoint spout has started committing the transaction
         * and the commit is in progress.
         */
        COMMITTING,
        /**
         * The checkpoint spout has started preparing the transaction for commit
         * and the prepare is in progress.
         */
        PREPARING
    }

    public enum Action {
        /**
         * prepare transaction for commit
         */
        PREPARE,
        /**
         * commit the previously prepared transaction
         */
        COMMIT,
        /**
         * rollback the previously prepared transaction
         */
        ROLLBACK,
        /**
         * initialize the state
         */
        INITSTATE
    }

    public CheckPointState(long txid, State state) {
        this.txid = txid;
        this.state = state;
    }

    // for kryo
    public CheckPointState() {
    }

    public long getTxid() {
        return txid;
    }

    public State getState() {
        return state;
    }

    /**
     * Get the next state based on this checkpoint state.
     *
     * @param recovering if in recovering phase
     * @return the next checkpoint state based on this state.
     */
    public CheckPointState nextState(boolean recovering) {
        CheckPointState nextState;
        switch (state) {
            case PREPARING:
                nextState = recovering ? new CheckPointState(txid - 1, COMMITTED) : new CheckPointState(txid, COMMITTING);
                break;
            case COMMITTING:
                nextState = new CheckPointState(txid, COMMITTED);
                break;
            case COMMITTED:
                nextState = recovering ? this : new CheckPointState(txid + 1, PREPARING);
                break;
            default:
                throw new IllegalStateException("Unknown state " + state);
        }
        return nextState;
    }

    /**
     * Get the next action to perform based on this checkpoint state.
     *
     * @param recovering if in recovering phase
     * @return the next action to perform based on this state
     */
    public Action nextAction(boolean recovering) {
        Action action;
        switch (state) {
            case PREPARING:
                action = recovering ? Action.ROLLBACK : Action.PREPARE;
                break;
            case COMMITTING:
                action = Action.COMMIT;
                break;
            case COMMITTED:
                action = recovering ? Action.INITSTATE : Action.PREPARE;
                break;
            default:
                throw new IllegalStateException("Unknown state " + state);
        }
        return action;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CheckPointState that = (CheckPointState) o;

        if (txid != that.txid) return false;
        return state == that.state;

    }

    @Override
    public int hashCode() {
        int result = (int) (txid ^ (txid >>> 32));
        result = 31 * result + (state != null ? state.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "CheckPointState{" +
                "txid=" + txid +
                ", state=" + state +
                '}';
    }
}
