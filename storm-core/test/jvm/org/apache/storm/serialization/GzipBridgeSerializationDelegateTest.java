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
package org.apache.storm.serialization;

import static org.junit.Assert.*;

import java.io.Serializable;
import org.junit.Test;
import org.junit.Before;


public class GzipBridgeSerializationDelegateTest {

    SerializationDelegate testDelegate;

    @Before
    public void setUp() throws Exception {
        testDelegate = new GzipBridgeSerializationDelegate();
    }

    @Test
    public void testDeserialize_readingFromGzip() throws Exception {
        TestPojo pojo = new TestPojo();
        pojo.name = "foo";
        pojo.age = 100;

        byte[] serialized = new GzipSerializationDelegate().serialize(pojo);

        TestPojo pojo2 = (TestPojo)testDelegate.deserialize(serialized, TestPojo.class);

        assertEquals(pojo2.name, pojo.name);
        assertEquals(pojo2.age, pojo.age);
    }

    @Test
    public void testDeserialize_readingFromGzipBridge() throws Exception {
        TestPojo pojo = new TestPojo();
        pojo.name = "bar";
        pojo.age = 200;

        byte[] serialized = new GzipBridgeSerializationDelegate().serialize(pojo);

        TestPojo pojo2 = (TestPojo)testDelegate.deserialize(serialized, TestPojo.class);

        assertEquals(pojo2.name, pojo.name);
        assertEquals(pojo2.age, pojo.age);
    }

    @Test
    public void testDeserialize_readingFromDefault() throws Exception {
        TestPojo pojo = new TestPojo();
        pojo.name = "baz";
        pojo.age = 300;

        byte[] serialized = new DefaultSerializationDelegate().serialize(pojo);

        TestPojo pojo2 = (TestPojo)testDelegate.deserialize(serialized, TestPojo.class);

        assertEquals(pojo2.name, pojo.name);
        assertEquals(pojo2.age, pojo.age);
    }

    static class TestPojo implements Serializable {
        String name;
        int age;
    }
}
