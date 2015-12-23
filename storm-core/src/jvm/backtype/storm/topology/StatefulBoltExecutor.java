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
package backtype.storm.topology;

import backtype.storm.spout.CheckpointSpout;
import backtype.storm.state.State;
import backtype.storm.state.StateFactory;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static backtype.storm.spout.CheckPointState.Action;
import static backtype.storm.spout.CheckPointState.Action.COMMIT;
import static backtype.storm.spout.CheckPointState.Action.PREPARE;
import static backtype.storm.spout.CheckPointState.Action.ROLLBACK;
import static backtype.storm.spout.CheckPointState.Action.INITSTATE;

/**
 * Wraps a {@link IStatefulBolt} and manages the state of the bolt.
 */
public class StatefulBoltExecutor<T extends State> extends CheckpointTupleForwarder {
    private static final Logger LOG = LoggerFactory.getLogger(StatefulBoltExecutor.class);
    private final IStatefulBolt<T> bolt;
    private State state;
    private boolean boltInitialized = false;
    private List<Tuple> pendingTuples = new ArrayList<>();

    public StatefulBoltExecutor(IStatefulBolt<T> bolt) {
        super(bolt);
        this.bolt = bolt;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        // get the last successfully committed state from state store
        String namespace = context.getThisComponentId() + "-" + context.getThisTaskId();
        prepare(stormConf, context, collector, StateFactory.getState(namespace, stormConf, context));
    }

    // package access for unit tests
    void prepare(Map stormConf, TopologyContext context, OutputCollector collector, State state) {
        super.prepare(stormConf, context, collector);
        this.state = state;
    }

    @Override
    protected void handleCheckpoint(Tuple input, Action action, long txid) {
        LOG.debug("handleCheckPoint with tuple {}, action {}, txid {}", input, action, txid);
        if (action == PREPARE) {
            bolt.prePrepare(txid);
            state.prepareCommit(txid);
        } else if (action == COMMIT) {
            bolt.preCommit(txid);
            state.commit(txid);
        } else if (action == ROLLBACK) {
            bolt.preRollback();
            state.rollback();
        } else if (action == INITSTATE) {
            bolt.initState((T) state);
            boltInitialized = true;
            LOG.debug("{} pending tuples to process", pendingTuples.size());
            for (Tuple tuple : pendingTuples) {
                bolt.execute(tuple);
            }
        }
        collector.emit(CheckpointSpout.CHECKPOINT_STREAM_ID, input, new Values(txid, action));
    }

    @Override
    protected void handleTuple(Tuple input) {
        if (boltInitialized) {
            bolt.execute(input);
        } else {
            LOG.debug("Bolt state not initialized, adding tuple {} to pending tuples", input);
            pendingTuples.add(input);
        }
    }
}
