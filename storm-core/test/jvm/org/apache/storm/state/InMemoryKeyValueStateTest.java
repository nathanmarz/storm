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
package org.apache.storm.state;

import org.apache.storm.state.InMemoryKeyValueState;
import org.apache.storm.state.KeyValueState;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link InMemoryKeyValueState}
 */
public class InMemoryKeyValueStateTest {

    KeyValueState<String, String> keyValueState;

    @Before
    public void setUp() {
        keyValueState = new InMemoryKeyValueState<>();
    }

    @Test
    public void testPutAndGet() throws Exception {
        keyValueState.put("a", "1");
        keyValueState.put("b", "2");
        assertArrayEquals(new String[]{"1", "2", null}, getValues());
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
}