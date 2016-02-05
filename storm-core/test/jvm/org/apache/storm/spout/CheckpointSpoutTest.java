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

import org.apache.storm.Config;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.state.StateFactory;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

import static org.apache.storm.spout.CheckPointState.State.COMMITTED;
import static org.junit.Assert.assertEquals;
import static org.apache.storm.spout.CheckPointState.Action;

/**
 * Unit test for {@link CheckpointSpout}
 */
public class CheckpointSpoutTest {
    CheckpointSpout spout = new CheckpointSpout();
    TopologyContext mockTopologyContext;
    SpoutOutputCollector mockOutputCollector;

    @Before
    public void setUp() throws Exception {
        mockTopologyContext = Mockito.mock(TopologyContext.class);
        Mockito.when(mockTopologyContext.getThisComponentId()).thenReturn("test");
        Mockito.when(mockTopologyContext.getThisTaskId()).thenReturn(1);
        mockOutputCollector = Mockito.mock(SpoutOutputCollector.class);
    }

    @Test
    public void testInitState() throws Exception {
        spout.open(new HashMap(), mockTopologyContext, mockOutputCollector);

        spout.nextTuple();
        Values expectedTuple = new Values(-1L, Action.INITSTATE);
        ArgumentCaptor<String> stream = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Values> values = ArgumentCaptor.forClass(Values.class);
        ArgumentCaptor<Object> msgId = ArgumentCaptor.forClass(Object.class);
        Mockito.verify(mockOutputCollector).emit(stream.capture(),
                                                 values.capture(),
                                                 msgId.capture());

        assertEquals(CheckpointSpout.CHECKPOINT_STREAM_ID, stream.getValue());
        assertEquals(expectedTuple, values.getValue());
        assertEquals(-1L, msgId.getValue());

        spout.ack(-1L);

        Mockito.verify(mockOutputCollector).emit(stream.capture(),
                                                 values.capture(),
                                                 msgId.capture());

        expectedTuple = new Values(-1L, Action.INITSTATE);
        assertEquals(CheckpointSpout.CHECKPOINT_STREAM_ID, stream.getValue());
        assertEquals(expectedTuple, values.getValue());
        assertEquals(-1L, msgId.getValue());

    }

    @Test
    public void testPrepare() throws Exception {
        spout.open(new HashMap(), mockTopologyContext, mockOutputCollector);
        ArgumentCaptor<String> stream = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Values> values = ArgumentCaptor.forClass(Values.class);
        ArgumentCaptor<Object> msgId = ArgumentCaptor.forClass(Object.class);

        spout.nextTuple();
        spout.ack(-1L);
        spout.nextTuple();
        Mockito.verify(mockOutputCollector, Mockito.times(2)).emit(stream.capture(),
                                                 values.capture(),
                                                 msgId.capture());

        Values expectedTuple = new Values(0L, Action.PREPARE);
        assertEquals(CheckpointSpout.CHECKPOINT_STREAM_ID, stream.getValue());
        assertEquals(expectedTuple, values.getValue());
        assertEquals(0L, msgId.getValue());

    }

    @Test
    public void testPrepareWithFail() throws Exception {
        Map<String, Object> stormConf = new HashMap<>();
        KeyValueState<String, CheckPointState> state =
                (KeyValueState<String, CheckPointState>) StateFactory.getState("__state", stormConf, mockTopologyContext);
        CheckPointState txState = new CheckPointState(-1, COMMITTED);
        state.put("__state", txState);

        spout.open(mockTopologyContext, mockOutputCollector, 0, state);
        ArgumentCaptor<String> stream = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Values> values = ArgumentCaptor.forClass(Values.class);
        ArgumentCaptor<Object> msgId = ArgumentCaptor.forClass(Object.class);

        spout.nextTuple();
        spout.ack(-1L);
        Utils.sleep(10);
        spout.nextTuple();
        spout.ack(0L);
        Utils.sleep(10);
        spout.nextTuple();
        spout.ack(0L);
        Utils.sleep(10);
        spout.nextTuple();
        spout.fail(1L);
        Utils.sleep(10);
        spout.nextTuple();
        spout.fail(1L);
        Utils.sleep(10);
        spout.nextTuple();
        spout.ack(1L);
        Utils.sleep(10);
        spout.nextTuple();
        spout.ack(0L);
        Utils.sleep(10);
        spout.nextTuple();
        Mockito.verify(mockOutputCollector, Mockito.times(8)).emit(stream.capture(),
                                                                   values.capture(),
                                                                   msgId.capture());

        Values expectedTuple = new Values(1L, Action.PREPARE);
        assertEquals(CheckpointSpout.CHECKPOINT_STREAM_ID, stream.getValue());
        assertEquals(expectedTuple, values.getValue());
        assertEquals(1L, msgId.getValue());

    }

    @Test
    public void testCommit() throws Exception {
        Map<String, Object> stormConf = new HashMap();
        stormConf.put(Config.TOPOLOGY_STATE_CHECKPOINT_INTERVAL, 0);
        spout.open(stormConf, mockTopologyContext, mockOutputCollector);
        ArgumentCaptor<String> stream = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Values> values = ArgumentCaptor.forClass(Values.class);
        ArgumentCaptor<Object> msgId = ArgumentCaptor.forClass(Object.class);

        spout.nextTuple();
        spout.ack(-1L);
        spout.nextTuple();
        spout.ack(0L);
        Utils.sleep(10);
        spout.nextTuple();
        spout.fail(0L);
        Utils.sleep(10);
        spout.nextTuple();
        Mockito.verify(mockOutputCollector, Mockito.times(4)).emit(stream.capture(),
                                                                   values.capture(),
                                                                   msgId.capture());

        Values expectedTuple = new Values(0L, Action.COMMIT);
        assertEquals(CheckpointSpout.CHECKPOINT_STREAM_ID, stream.getValue());
        assertEquals(expectedTuple, values.getValue());
        assertEquals(0L, msgId.getValue());

    }

    @Test
    public void testRecoveryRollback() throws Exception {
        Map<String, Object> stormConf = new HashMap();

        KeyValueState<String, CheckPointState> state =
                (KeyValueState<String, CheckPointState>) StateFactory.getState("test-1", stormConf, mockTopologyContext);

        CheckPointState checkPointState = new CheckPointState(100, CheckPointState.State.PREPARING);
        state.put("__state", checkPointState);
        spout.open(mockTopologyContext, mockOutputCollector, 0, state);
        ArgumentCaptor<String> stream = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Values> values = ArgumentCaptor.forClass(Values.class);
        ArgumentCaptor<Object> msgId = ArgumentCaptor.forClass(Object.class);

        spout.nextTuple();
        Mockito.verify(mockOutputCollector, Mockito.times(1)).emit(stream.capture(),
                                                                   values.capture(),
                                                                   msgId.capture());

        Values expectedTuple = new Values(100L, Action.ROLLBACK);
        assertEquals(CheckpointSpout.CHECKPOINT_STREAM_ID, stream.getValue());
        assertEquals(expectedTuple, values.getValue());
        assertEquals(100L, msgId.getValue());

    }

    @Test
    public void testRecoveryRollbackAck() throws Exception {
        Map<String, Object> stormConf = new HashMap();

        KeyValueState<String, CheckPointState> state =
                (KeyValueState<String, CheckPointState>) StateFactory.getState("test-1", stormConf, mockTopologyContext);

        CheckPointState checkPointState = new CheckPointState(100, CheckPointState.State.PREPARING);
        state.put("__state", checkPointState);
        spout.open(mockTopologyContext, mockOutputCollector, 0, state);
        ArgumentCaptor<String> stream = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Values> values = ArgumentCaptor.forClass(Values.class);
        ArgumentCaptor<Object> msgId = ArgumentCaptor.forClass(Object.class);

        spout.nextTuple();
        spout.ack(100L);
        spout.nextTuple();
        spout.ack(99L);
        spout.nextTuple();
        Mockito.verify(mockOutputCollector, Mockito.times(3)).emit(stream.capture(),
                                                                   values.capture(),
                                                                   msgId.capture());

        Values expectedTuple = new Values(100L, Action.PREPARE);
        assertEquals(CheckpointSpout.CHECKPOINT_STREAM_ID, stream.getValue());
        assertEquals(expectedTuple, values.getValue());
        assertEquals(100L, msgId.getValue());

    }

    @Test
    public void testRecoveryCommit() throws Exception {
        Map<String, Object> stormConf = new HashMap();

        KeyValueState<String, CheckPointState> state =
                (KeyValueState<String, CheckPointState>) StateFactory.getState("test-1", stormConf, mockTopologyContext);

        CheckPointState checkPointState = new CheckPointState(100, CheckPointState.State.COMMITTING);
        state.put("__state", checkPointState);
        spout.open(mockTopologyContext, mockOutputCollector, 0, state);
        ArgumentCaptor<String> stream = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Values> values = ArgumentCaptor.forClass(Values.class);
        ArgumentCaptor<Object> msgId = ArgumentCaptor.forClass(Object.class);

        spout.nextTuple();
        Mockito.verify(mockOutputCollector, Mockito.times(1)).emit(stream.capture(),
                                                                   values.capture(),
                                                                   msgId.capture());

        Values expectedTuple = new Values(100L, Action.COMMIT);
        assertEquals(CheckpointSpout.CHECKPOINT_STREAM_ID, stream.getValue());
        assertEquals(expectedTuple, values.getValue());
        assertEquals(100L, msgId.getValue());

    }

}