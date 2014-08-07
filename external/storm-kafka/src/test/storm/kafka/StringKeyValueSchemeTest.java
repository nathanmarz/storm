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
package storm.kafka;

import backtype.storm.tuple.Fields;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StringKeyValueSchemeTest {

    private StringKeyValueScheme scheme = new StringKeyValueScheme();

    @Test
    public void testDeserialize() throws Exception {
        assertEquals(Arrays.asList("test"), scheme.deserialize("test".getBytes()));
    }

    @Test
    public void testGetOutputFields() throws Exception {
        Fields outputFields = scheme.getOutputFields();
        assertTrue(outputFields.contains(StringScheme.STRING_SCHEME_KEY));
        assertEquals(1, outputFields.size());
    }

    @Test
    public void testDeserializeWithNullKeyAndValue() throws Exception {
        assertEquals(Arrays.asList("test"), scheme.deserializeKeyAndValue(null, "test".getBytes()));
    }

    @Test
    public void testDeserializeWithKeyAndValue() throws Exception {
        assertEquals(Arrays.asList(ImmutableMap.of("key", "test")),
                scheme.deserializeKeyAndValue("key".getBytes(), "test".getBytes()));
    }
}
