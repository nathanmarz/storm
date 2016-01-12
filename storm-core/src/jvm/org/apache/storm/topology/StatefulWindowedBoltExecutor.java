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

import org.apache.storm.Config;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.state.State;
import org.apache.storm.state.StateFactory;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.WindowLifecycleListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Wraps a {@link IStatefulWindowedBolt} and handles the execution. Saves the last expired
 * and evaluated states of the window during checkpoint and restores the state during recovery.
 */
public class StatefulWindowedBoltExecutor<T extends State> extends WindowedBoltExecutor implements IStatefulBolt<T> {
    private static final Logger LOG = LoggerFactory.getLogger(StatefulWindowedBoltExecutor.class);
    private final IStatefulWindowedBolt<T> statefulWindowedBolt;
    private transient String msgIdFieldName;
    private transient TopologyContext topologyContext;
    private transient OutputCollector outputCollector;
    // last evaluated and last expired message ids per task stream (source taskid + stream-id)
    private transient KeyValueState<TaskStream, WindowState> streamState;
    private transient List<Tuple> pendingTuples;
    // the states to be recovered
    private transient Map<TaskStream, WindowState> recoveryStates;
    private transient boolean stateInitialized;
    private transient boolean prePrepared;

    public StatefulWindowedBoltExecutor(IStatefulWindowedBolt<T> bolt) {
        super(bolt);
        this.statefulWindowedBolt = bolt;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        prepare(stormConf, context, collector, getWindowState(stormConf, context));
    }

    // package access for unit tests
    void prepare(Map stormConf, TopologyContext context, OutputCollector collector,
                 KeyValueState<TaskStream, WindowState> windowState) {
        init(stormConf, context, collector, windowState);
        super.prepare(stormConf, context, collector);
    }

    private void init(Map stormConf, TopologyContext context, OutputCollector collector,
                      KeyValueState<TaskStream, WindowState> windowState) {
        if (stormConf.containsKey(Config.TOPOLOGY_BOLTS_MESSAGE_ID_FIELD_NAME)) {
            msgIdFieldName = (String) stormConf.get(Config.TOPOLOGY_BOLTS_MESSAGE_ID_FIELD_NAME);
        } else {
            throw new IllegalArgumentException(Config.TOPOLOGY_BOLTS_MESSAGE_ID_FIELD_NAME + " is not set");
        }
        topologyContext = context;
        outputCollector = collector;
        streamState = windowState;
        pendingTuples = new ArrayList<>();
        recoveryStates = new HashMap<>();
        stateInitialized = false;
        prePrepared = false;
    }

    @Override
    public void execute(Tuple input) {
        if (!isStateInitialized()) {
            throw new IllegalStateException("execute invoked before initState with input tuple " + input);
        } else if (isRecovering()) {
            handleRecovery(input);
        } else {
            super.execute(input);
        }
    }

    @Override
    protected void start() {
        if (!isStateInitialized() || isRecovering()) {
            LOG.debug("Will invoke start after recovery is complete.");
        } else {
            super.start();
        }
    }

    private void handleRecovery(Tuple input) {
        long msgId = getMsgId(input);
        TaskStream taskStream = TaskStream.fromTuple(input);
        WindowState state = recoveryStates.get(taskStream);
        LOG.debug("handleRecovery, recoveryStates {}", recoveryStates);
        if (state != null) {
            LOG.debug("Tuple msgid {}, saved state {}", msgId, state);
            if (msgId <= state.lastExpired) {
                LOG.debug("Ignoring tuple since msg id {} <= lastExpired id {}", msgId, state.lastExpired);
                outputCollector.ack(input);
            } else if (msgId <= state.lastEvaluated) {
                super.execute(input);
            } else {
                LOG.debug("Tuple msg id {} > lastEvaluated id {}, adding to pendingTuples and clearing recovery state " +
                                  "for taskStream {}", msgId, state.lastEvaluated, taskStream);
                pendingTuples.add(input);
                clearRecoveryState(taskStream);
            }
        } else {
            pendingTuples.add(input);
        }
    }

    private void clearRecoveryState(TaskStream stream) {
        recoveryStates.remove(stream);
        if (!isRecovering()) {
            super.start();
            LOG.debug("Recovery complete, processing {} pending tuples", pendingTuples.size());
            for (Tuple tuple : pendingTuples) {
                super.execute(tuple);
            }
        }
    }

    private boolean isRecovering() {
        return !recoveryStates.isEmpty();
    }

    private boolean isStateInitialized() {
        return stateInitialized;
    }

    @Override
    public void initState(T state) {
        if (stateInitialized) {
            LOG.warn("State is already initialized. Ignoring initState");
            return;
        }
        statefulWindowedBolt.initState((T) state);
        // query the streamState for each input task stream and compute recoveryStates
        for (GlobalStreamId streamId : topologyContext.getThisSources().keySet()) {
            for (int taskId : topologyContext.getComponentTasks(streamId.get_componentId())) {
                WindowState windowState = streamState.get(new TaskStream(taskId, streamId));
                if (windowState != null) {
                    recoveryStates.put(new TaskStream(taskId, streamId), windowState);
                }
            }
        }
        LOG.debug("recoveryStates {}", recoveryStates);
        stateInitialized = true;
        start();
    }

    @Override
    public void preCommit(long txid) {
        if (!isStateInitialized() || (!isRecovering() && prePrepared)) {
            LOG.debug("Commit streamState, txid {}", txid);
            streamState.commit(txid);
        } else {
            LOG.debug("Still recovering, ignoring preCommit and not committing streamState.");
        }
    }

    @Override
    public void prePrepare(long txid) {
        if (!isStateInitialized()) {
            LOG.warn("Cannot prepare before initState");
        } else if (!isRecovering()) {
            LOG.debug("Prepare streamState, txid {}", txid);
            streamState.prepareCommit(txid);
            prePrepared = true;
        } else {
            LOG.debug("Still recovering, ignoring prePrepare and not preparing streamState.");
        }
    }

    @Override
    public void preRollback() {
        LOG.debug("Rollback streamState, stateInitialized {}", stateInitialized);
        streamState.rollback();
    }

    @Override
    protected WindowLifecycleListener<Tuple> newWindowLifecycleListener() {
        final WindowLifecycleListener<Tuple> parentListener = super.newWindowLifecycleListener();
        return new WindowLifecycleListener<Tuple>() {
            @Override
            public void onExpiry(List<Tuple> events) {
                parentListener.onExpiry(events);
            }

            @Override
            public void onActivation(List<Tuple> events, List<Tuple> newEvents, List<Tuple> expired) {
                if (isRecovering()) {
                    String msg = String.format("Unexpected activation with events %s, newEvents %s, expired %s in recovering state. " +
                                                       "recoveryStates %s ", events, newEvents, expired, recoveryStates);
                    LOG.error(msg);
                    throw new IllegalStateException(msg);
                } else {
                    parentListener.onActivation(events, newEvents, expired);
                    updateWindowState(expired, newEvents);
                }
            }
        };
    }

    private void updateWindowState(List<Tuple> expired, List<Tuple> newEvents) {
        LOG.debug("Update window state, {} expired, {} new events", expired.size(), newEvents.size());
        Map<TaskStream, WindowState> state = new HashMap<>();
        updateState(state, expired, false);
        updateState(state, newEvents, true);
        updateStreamState(state);
    }

    private void updateState(Map<TaskStream, WindowState> state, List<Tuple> tuples, boolean newEvents) {
        for (Tuple tuple : tuples) {
            TaskStream taskStream = TaskStream.fromTuple(tuple);
            WindowState curState = state.get(taskStream);
            WindowState newState;
            if ((newState = getUpdatedState(curState, getMsgId(tuple), newEvents)) != null) {
                state.put(taskStream, newState);
            }
        }
    }

    // update streamState based on stateUpdates
    private void updateStreamState(Map<TaskStream, WindowState> state) {
        for (Map.Entry<TaskStream, WindowState> entry : state.entrySet()) {
            TaskStream taskStream = entry.getKey();
            WindowState newState = entry.getValue();
            WindowState curState = streamState.get(taskStream);
            if (curState == null) {
                streamState.put(taskStream, newState);
            } else {
                WindowState updatedState = new WindowState(Math.max(newState.lastExpired, curState.lastExpired),
                                                           Math.max(newState.lastEvaluated, curState.lastEvaluated));
                LOG.debug("Update window state, taskStream {}, curState {}, newState {}", taskStream, curState, updatedState);
                streamState.put(taskStream, updatedState);
            }
        }
    }

    private WindowState getUpdatedState(WindowState state, long msgId, boolean newEvents) {
        WindowState result = null;
        if (newEvents) {
            if (state == null) {
                result = new WindowState(Long.MIN_VALUE, msgId);
            } else if (msgId > state.lastEvaluated) {
                result = new WindowState(state.lastExpired, msgId);
            }
        } else {
            if (state == null) {
                result = new WindowState(msgId, Long.MIN_VALUE);
            } else if (msgId > state.lastExpired) {
                result = new WindowState(msgId, state.lastEvaluated);
            }
        }
        return result;
    }

    private long getMsgId(Tuple input) {
        return input.getLongByField(msgIdFieldName);
    }

    private KeyValueState<TaskStream, WindowState> getWindowState(Map stormConf, TopologyContext context) {
        String namespace = context.getThisComponentId() + "-" + context.getThisTaskId() + "-window";
        return (KeyValueState<TaskStream, WindowState>) StateFactory.getState(namespace, stormConf, context);
    }

    static class WindowState {
        private long lastExpired;
        private long lastEvaluated;

        // for kryo
        WindowState() {
        }

        WindowState(long lastExpired, long lastEvaluated) {
            this.lastExpired = lastExpired;
            this.lastEvaluated = lastEvaluated;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            WindowState that = (WindowState) o;

            if (lastExpired != that.lastExpired) return false;
            return lastEvaluated == that.lastEvaluated;

        }

        @Override
        public int hashCode() {
            int result = (int) (lastExpired ^ (lastExpired >>> 32));
            result = 31 * result + (int) (lastEvaluated ^ (lastEvaluated >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return "WindowState{" +
                    "lastExpired=" + lastExpired +
                    ", lastEvaluated=" + lastEvaluated +
                    '}';
        }
    }

    static class TaskStream {
        private int sourceTask;
        private GlobalStreamId streamId;

        // for kryo
        TaskStream() {
        }

        TaskStream(int sourceTask, GlobalStreamId streamId) {
            this.sourceTask = sourceTask;
            this.streamId = streamId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TaskStream that = (TaskStream) o;

            if (sourceTask != that.sourceTask) return false;
            return streamId != null ? streamId.equals(that.streamId) : that.streamId == null;

        }

        @Override
        public int hashCode() {
            int result = streamId != null ? streamId.hashCode() : 0;
            result = 31 * result + sourceTask;
            return result;
        }

        @Override
        public String toString() {
            return "TaskStream{" +
                    "sourceTask=" + sourceTask +
                    ", streamId=" + streamId +
                    '}';
        }

        static TaskStream fromTuple(Tuple input) {
            return new TaskStream(input.getSourceTask(), input.getSourceGlobalStreamId());
        }
    }
}
