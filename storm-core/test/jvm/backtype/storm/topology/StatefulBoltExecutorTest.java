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

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.spout.CheckpointSpout;
import backtype.storm.state.KeyValueState;
import backtype.storm.state.State;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static backtype.storm.spout.CheckpointSpout.*;
import static backtype.storm.spout.CheckPointState.Action.INITSTATE;
import static backtype.storm.spout.CheckPointState.Action.ROLLBACK;
import static backtype.storm.spout.CheckPointState.Action.COMMIT;

/**
 * Unit tests for {@link StatefulBoltExecutor}
 */
public class StatefulBoltExecutorTest {
    StatefulBoltExecutor<KeyValueState<String, String>> executor;
    IStatefulBolt<KeyValueState<String, String>> mockBolt;
    TopologyContext mockTopologyContext;
    Tuple mockTuple;
    Tuple mockCheckpointTuple;
    Map<String, Object> mockStormConf = new HashMap<>();
    OutputCollector mockOutputCollector;
    State mockState;
    Map<GlobalStreamId, Grouping> mockGlobalStream;
    Set<GlobalStreamId> mockStreamIds;
    @Before
    public void setUp() throws Exception {
        mockBolt = Mockito.mock(IStatefulBolt.class);
        executor = new StatefulBoltExecutor<>(mockBolt);
        GlobalStreamId mockGlobalStreamId = Mockito.mock(GlobalStreamId.class);
        Mockito.when(mockGlobalStreamId.get_streamId()).thenReturn(CheckpointSpout.CHECKPOINT_STREAM_ID);
        mockStreamIds = new HashSet<>();
        mockStreamIds.add(mockGlobalStreamId);
        mockTopologyContext = Mockito.mock(TopologyContext.class);
        mockOutputCollector = Mockito.mock(OutputCollector.class);
        mockGlobalStream = Mockito.mock(Map.class);
        mockState = Mockito.mock(State.class);
        Mockito.when(mockTopologyContext.getThisComponentId()).thenReturn("test");
        Mockito.when(mockTopologyContext.getThisTaskId()).thenReturn(1);
        Mockito.when(mockTopologyContext.getThisSources()).thenReturn(mockGlobalStream);
        Mockito.when(mockTopologyContext.getComponentTasks(Mockito.anyString())).thenReturn(Collections.singletonList(1));
        Mockito.when(mockGlobalStream.keySet()).thenReturn(mockStreamIds);
        mockTuple = Mockito.mock(Tuple.class);
        mockCheckpointTuple = Mockito.mock(Tuple.class);
        executor.prepare(mockStormConf, mockTopologyContext, mockOutputCollector, mockState);
    }

    @Test
    public void testHandleTupleBeforeInit() throws Exception {
        Mockito.when(mockTuple.getSourceStreamId()).thenReturn("default");
        executor.execute(mockTuple);
        Mockito.verify(mockBolt, Mockito.times(0)).execute(Mockito.any(Tuple.class));
    }


    @Test
    public void testHandleTuple() throws Exception {
        Mockito.when(mockTuple.getSourceStreamId()).thenReturn("default");
        executor.execute(mockTuple);
        Mockito.when(mockCheckpointTuple.getSourceStreamId()).thenReturn(CheckpointSpout.CHECKPOINT_STREAM_ID);
        Mockito.when(mockCheckpointTuple.getValueByField(CHECKPOINT_FIELD_ACTION)).thenReturn(INITSTATE);
        Mockito.when(mockCheckpointTuple.getLongByField(CHECKPOINT_FIELD_TXID)).thenReturn(new Long(0));
        Mockito.doNothing().when(mockOutputCollector).ack(mockCheckpointTuple);
        executor.execute(mockCheckpointTuple);
        Mockito.verify(mockBolt, Mockito.times(1)).execute(mockTuple);
        Mockito.verify(mockBolt, Mockito.times(1)).initState(Mockito.any(KeyValueState.class));
    }

    @Test
    public void testRollback() throws Exception {
        Mockito.when(mockTuple.getSourceStreamId()).thenReturn("default");
        executor.execute(mockTuple);
        Mockito.when(mockCheckpointTuple.getSourceStreamId()).thenReturn(CheckpointSpout.CHECKPOINT_STREAM_ID);
        Mockito.when(mockCheckpointTuple.getValueByField(CHECKPOINT_FIELD_ACTION)).thenReturn(ROLLBACK);
        Mockito.when(mockCheckpointTuple.getLongByField(CHECKPOINT_FIELD_TXID)).thenReturn(new Long(0));
        Mockito.doNothing().when(mockOutputCollector).ack(mockCheckpointTuple);
        executor.execute(mockCheckpointTuple);
        Mockito.verify(mockState, Mockito.times(1)).rollback();
    }

    @Test
    public void testCommit() throws Exception {
        Mockito.when(mockTuple.getSourceStreamId()).thenReturn("default");
        executor.execute(mockTuple);
        Mockito.when(mockCheckpointTuple.getSourceStreamId()).thenReturn(CheckpointSpout.CHECKPOINT_STREAM_ID);
        Mockito.when(mockCheckpointTuple.getValueByField(CHECKPOINT_FIELD_ACTION)).thenReturn(COMMIT);
        Mockito.when(mockCheckpointTuple.getLongByField(CHECKPOINT_FIELD_TXID)).thenReturn(new Long(0));
        Mockito.doNothing().when(mockOutputCollector).ack(mockCheckpointTuple);
        executor.execute(mockCheckpointTuple);
        Mockito.verify(mockBolt, Mockito.times(1)).preCommit(new Long(0));
        Mockito.verify(mockState, Mockito.times(1)).commit(new Long(0));
    }
}