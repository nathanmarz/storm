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
package org.apache.storm.redis.state;

import org.apache.storm.state.DefaultStateSerializer;
import org.apache.storm.redis.common.container.JedisCommandsInstanceContainer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import redis.clients.jedis.BinaryClient;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link RedisKeyValueState}
 */
public class RedisKeyValueStateTest {
    JedisCommandsInstanceContainer mockContainer;
    JedisCommands mockCommands;
    RedisKeyValueState<String, String> keyValueState;

    @Before
    public void setUp() {
        final Map<String, Map<String, String>> mockMap = new HashMap<>();
        mockContainer = Mockito.mock(JedisCommandsInstanceContainer.class);
        mockCommands = Mockito.mock(JedisCommands.class);
        Mockito.when(mockContainer.getInstance()).thenReturn(mockCommands);
        ArgumentCaptor<String> stringArgumentCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> stringArgumentCaptor2 = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Map> mapArgumentCaptor = ArgumentCaptor.forClass(Map.class);

        Mockito.when(mockCommands.exists(Mockito.anyString()))
                .thenAnswer(new Answer<Boolean>() {
                    @Override
                    public Boolean answer(InvocationOnMock invocation) throws Throwable {
                        Object[] args = invocation.getArguments();
                        return mockMap.containsKey((String) args[0]);
                    }
                });


        Mockito.when(mockCommands.hmset(Mockito.anyString(), Mockito.anyMap()))
                .thenAnswer(new Answer<String>() {
                    @Override
                    public String answer(InvocationOnMock invocation) throws Throwable {
                        Object[] args = invocation.getArguments();
                        return hmset(mockMap, (String) args[0], (Map) args[1]);
                    }
                });

        Mockito.when(mockCommands.del(Mockito.anyString()))
                .thenAnswer(new Answer<Long>() {
                    @Override
                    public Long answer(InvocationOnMock invocation) throws Throwable {
                        Object[] args = invocation.getArguments();
                        return del(mockMap, (String) args[0]);
                    }
                });

        Mockito.when(mockCommands.hget(Mockito.anyString(), Mockito.anyString()))
                .thenAnswer(new Answer<String>() {
                    @Override
                    public String answer(InvocationOnMock invocation) throws Throwable {
                        Object[] args = invocation.getArguments();
                        return hget(mockMap, (String) args[0], (String) args[1]);
                    }
                });

        keyValueState = new RedisKeyValueState<String, String>("test", mockContainer, new DefaultStateSerializer<String>(),
                                                               new DefaultStateSerializer<String>());
    }


    @Test
    public void testPutAndGet() throws Exception {
        keyValueState.put("a", "1");
        keyValueState.put("b", "2");
        assertEquals("1", keyValueState.get("a"));
        assertEquals("2", keyValueState.get("b"));
        assertEquals(null, keyValueState.get("c"));
    }

    @Test
    public void testPrepareCommitRollback() throws Exception {
        keyValueState.put("a", "1");
        keyValueState.put("b", "2");
        keyValueState.prepareCommit(1);
        keyValueState.put("c", "3");
        assertArrayEquals(new String[]{"1", "2", "3"}, getValues());
        keyValueState.rollback();
        assertArrayEquals(new String[]{null, null, null}, getValues());
        keyValueState.put("a", "1");
        keyValueState.put("b", "2");
        keyValueState.prepareCommit(1);
        keyValueState.commit(1);
        keyValueState.put("c", "3");
        assertArrayEquals(new String[]{"1", "2", "3"}, getValues());
        keyValueState.rollback();
        assertArrayEquals(new String[]{"1", "2", null}, getValues());
    }

    private String[] getValues() {
        return new String[]{
                keyValueState.get("a"),
                keyValueState.get("b"),
                keyValueState.get("c")
        };
    }

    private String hmset(Map<String, Map<String, String>> mockMap, String key, Map value) {
        mockMap.put(key, value);
        return "";
    }

    private Long del(Map<String, Map<String, String>> mockMap, String key) {
        mockMap.remove(key);
        return 0L;
    }

    private String hget(Map<String, Map<String, String>> mockMap, String namespace, String key) {
        if (mockMap.containsKey(namespace)) {
            return mockMap.get(namespace).get(key);
        }
        return null;
    }

}