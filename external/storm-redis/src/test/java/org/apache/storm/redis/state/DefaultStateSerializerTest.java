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

import org.apache.storm.spout.CheckPointState;
import org.apache.storm.state.DefaultStateSerializer;
import org.apache.storm.state.Serializer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link DefaultStateSerializer}
 */
public class DefaultStateSerializerTest {

    @Test
    public void testSerializeDeserialize() throws Exception {
        Serializer<Long> s1 = new DefaultStateSerializer<Long>();
        byte[] bytes;
        long val = 100;
        bytes = s1.serialize(val);
        assertEquals(val, (long) s1.deserialize(bytes));

        Serializer<CheckPointState> s2 = new DefaultStateSerializer<CheckPointState>();
        CheckPointState cs = new CheckPointState(100, CheckPointState.State.COMMITTED);
        bytes = s2.serialize(cs);
        assertEquals(cs, (CheckPointState) s2.deserialize(bytes));

        List<Class<?>> classesToRegister = new ArrayList<>();
        classesToRegister.add(CheckPointState.class);
        Serializer<CheckPointState> s3 = new DefaultStateSerializer<CheckPointState>(classesToRegister);
        bytes = s2.serialize(cs);
        assertEquals(cs, (CheckPointState) s2.deserialize(bytes));

    }
}