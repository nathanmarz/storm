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
import org.apache.storm.spout.CheckpointSpout;
import org.apache.storm.task.IOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.storm.spout.CheckPointState.Action;
import static org.apache.storm.spout.CheckPointState.Action.ROLLBACK;
import static org.apache.storm.spout.CheckpointSpout.*;

/**
 * Wraps {@link IRichBolt} and forwards checkpoint tuples in a
 * stateful topology.
 * <p>
 * When a storm topology contains one or more {@link IStatefulBolt} all non-stateful
 * bolts are wrapped in {@link CheckpointTupleForwarder} so that the checkpoint tuples
 * can flow through the entire topology DAG.
 * </p>
 */
public class CheckpointTupleForwarder extends BaseStatefulBoltExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointTupleForwarder.class);
    private final IRichBolt bolt;

    public CheckpointTupleForwarder(IRichBolt bolt) {
        this.bolt = bolt;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector outputCollector) {
        init(context, new AnchoringOutputCollector(outputCollector));
        bolt.prepare(stormConf, context, collector);
    }

    @Override
    public void cleanup() {
        bolt.cleanup();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        bolt.declareOutputFields(declarer);
        declareCheckpointStream(declarer);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return bolt.getComponentConfiguration();
    }

    /**
     * Forwards the checkpoint tuple downstream.
     *
     * @param checkpointTuple  the checkpoint tuple
     * @param action the action (prepare, commit, rollback or initstate)
     * @param txid   the transaction id.
     */
    protected void handleCheckpoint(Tuple checkpointTuple, Action action, long txid) {
        collector.emit(CHECKPOINT_STREAM_ID, checkpointTuple, new Values(txid, action));
        collector.ack(checkpointTuple);
    }

    /**
     * Hands off tuple to the wrapped bolt to execute.
     *
     * <p>
     * Right now tuples continue to get forwarded while waiting for checkpoints to arrive on other streams
     * after checkpoint arrives on one of the streams. This can cause duplicates but still at least once.
     * </p>
     *
     * @param input the input tuple
     */
    protected void handleTuple(Tuple input) {
        bolt.execute(input);
    }
}
