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
package org.apache.storm.kafka;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

public class TestStringScheme {
  @Test
  public void testDeserializeString() {
    String s = "foo";
    byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
    ByteBuffer direct = ByteBuffer.allocateDirect(bytes.length);
    direct.put(bytes);
    direct.flip();
    String s1 = StringScheme.deserializeString(ByteBuffer.wrap(bytes));
    String s2 = StringScheme.deserializeString(direct);
    assertEquals(s, s1);
    assertEquals(s, s2);
  }
}
