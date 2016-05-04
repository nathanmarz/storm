/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.sql.kafka;

import org.apache.storm.utils.Utils;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestJsonRepresentation {
  @Test
  public void testJsonScheme() {
    final List<String> fields = Lists.newArrayList("ID", "val");
    final String s = "{\"ID\": 1, \"val\": \"2\"}";
    JsonScheme scheme = new JsonScheme(fields);
    List<Object> o = scheme.deserialize(ByteBuffer.wrap(s.getBytes(Charset.defaultCharset())));
    assertArrayEquals(new Object[] {1, "2"}, o.toArray());
  }

  @Test
  public void testJsonSerializer() {
    final List<String> fields = Lists.newArrayList("ID", "val");
    List<Object> o = Lists.<Object> newArrayList(1, "2");
    JsonSerializer s = new JsonSerializer(fields);
    ByteBuffer buf = s.write(o, null);
    byte[] b = Utils.toByteArray(buf);
    assertEquals("{\"ID\":1,\"val\":\"2\"}", new String(b));
  }
}
