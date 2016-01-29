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
import org.apache.storm.generated.Grouping;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;
import org.apache.storm.windowing.TupleWindowImpl;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.storm.topology.StatefulWindowedBoltExecutor.TaskStream;
import static org.apache.storm.topology.StatefulWindowedBoltExecutor.WindowState;

/**
 * Unit tests for {@link StatefulWindowedBoltExecutor}
 */
public class StatefulWindowedBoltExecutorTest {
    StatefulWindowedBoltExecutor<KeyValueState<String, String>> executor;

    IStatefulWindowedBolt<KeyValueState<String, String>> mockBolt;
    OutputCollector mockOutputCollector;
    TopologyContext mockTopologyContext;
    Map<String, Object> mockStormConf = new HashMap<>();

    @Before
    public void setUp() throws Exception {
        mockBolt = Mockito.mock(IStatefulWindowedBolt.class);
        mockTopologyContext = Mockito.mock(TopologyContext.class);
        mockOutputCollector = Mockito.mock(OutputCollector.class);
        executor = new StatefulWindowedBoltExecutor<>(mockBolt);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPrepare() throws Exception {
        executor.prepare(mockStormConf, mockTopologyContext, mockOutputCollector);
    }


    @Test
    public void testPrepareWithMsgid() throws Exception {
        mockStormConf.put(Config.TOPOLOGY_BOLTS_MESSAGE_ID_FIELD_NAME, "msgid");
        mockStormConf.put(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT, 5);
        mockStormConf.put(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_COUNT, 5);
        executor.prepare(mockStormConf, mockTopologyContext, mockOutputCollector);
    }

    @Test
    public void testExecute() throws Exception {
        mockStormConf.put(Config.TOPOLOGY_BOLTS_MESSAGE_ID_FIELD_NAME, "msgid");
        mockStormConf.put(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT, 5);
        mockStormConf.put(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_COUNT, 5);
        KeyValueState<TaskStream, WindowState> mockState;
        mockState = Mockito.mock(KeyValueState.class);
        executor.prepare(mockStormConf, mockTopologyContext, mockOutputCollector, mockState);
        executor.initState(null);
        List<Tuple> tuples = getMockTuples(5);
        for (Tuple tuple : tuples) {
            executor.execute(tuple);
        }
        Mockito.verify(mockBolt, Mockito.times(1)).execute(getTupleWindow(tuples));
        WindowState expectedState = new WindowState(Long.MIN_VALUE, 4);
        Mockito.verify(mockState, Mockito.times(1)).put(Mockito.any(TaskStream.class), Mockito.eq(expectedState));
    }

    @Test
    public void testRecovery() throws Exception {
        mockStormConf.put(Config.TOPOLOGY_BOLTS_MESSAGE_ID_FIELD_NAME, "msgid");
        mockStormConf.put(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT, 5);
        mockStormConf.put(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_COUNT, 5);
        KeyValueState<TaskStream, WindowState> mockState;
        mockState = Mockito.mock(KeyValueState.class);
        Map<GlobalStreamId, Grouping> mockMap = Mockito.mock(Map.class);
        Mockito.when(mockTopologyContext.getThisSources()).thenReturn(mockMap);
        Mockito.when(mockTopologyContext.getComponentTasks(Mockito.anyString())).thenReturn(Collections.singletonList(1));
        Mockito.when(mockMap.keySet()).thenReturn(Collections.singleton(new GlobalStreamId("a", "s")));
        WindowState mockWindowState = new WindowState(4, 4);
        Mockito.when(mockState.get(Mockito.any(TaskStream.class))).thenReturn(mockWindowState);
        executor.prepare(mockStormConf, mockTopologyContext, mockOutputCollector, mockState);
        executor.initState(null);
        List<Tuple> tuples = getMockTuples(10);
        for (Tuple tuple : tuples) {
            executor.execute(tuple);
        }
        WindowState expectedState = new WindowState(4, 9);
        Mockito.verify(mockState, Mockito.times(1)).put(Mockito.any(TaskStream.class), Mockito.eq(expectedState));
    }

    private TupleWindow getTupleWindow(List<Tuple> tuples) {
        return new TupleWindowImpl(tuples, tuples, Collections.<Tuple>emptyList());
    }

    private List<Tuple> getMockTuples(int count) {
        List<Tuple> mockTuples = new ArrayList<>();
        for (long i = 0; i < count; i++) {
            Tuple mockTuple = Mockito.mock(Tuple.class);
            Mockito.when(mockTuple.getLongByField("msgid")).thenReturn(i);
            Mockito.when(mockTuple.getSourceTask()).thenReturn(1);
            Mockito.when(mockTuple.getSourceGlobalStreamId()).thenReturn(new GlobalStreamId("a", "s"));
            mockTuples.add(mockTuple);
        }
        return mockTuples;
    }
}