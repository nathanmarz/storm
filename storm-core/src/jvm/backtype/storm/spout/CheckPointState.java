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

/**
 * Captures the current state of the transaction in
 * {@link CheckpointSpout}
 */
public class CheckPointState {
    public long txid;
    public State state;

    public enum State {
        /**
         * The checkpoint spout has committed the transaction.
         */
        COMMITTED,
        /**
         * The checkpoint spout has started committing the transaction.
         */
        COMMITTING,
        /**
         * The checkpoint spout has started preparing the transaction for commit.
         */
        PREPARING
    }

    public CheckPointState(long txid, State state) {
        this.txid = txid;
        this.state = state;
    }

    // for kryo
    public CheckPointState() {
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
