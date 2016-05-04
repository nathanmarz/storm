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
package org.apache.storm.hdfs.bolt;

import org.apache.storm.hdfs.common.AbstractHDFSWriter;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mock;

public class TestWritersMap {

    AbstractHdfsBolt.WritersMap map = new AbstractHdfsBolt.WritersMap(2);
    @Mock AbstractHDFSWriter foo;
    @Mock AbstractHDFSWriter bar;
    @Mock AbstractHDFSWriter baz;

    @Test public void testLRUBehavior()
    {
        map.put("FOO", foo);
        map.put("BAR", bar);

        //Access foo to make it most recently used
        map.get("FOO");

        //Add an element and bar should drop out
        map.put("BAZ", baz);

        Assert.assertTrue(map.keySet().contains("FOO"));
        Assert.assertTrue(map.keySet().contains("BAZ"));

        Assert.assertFalse(map.keySet().contains("BAR"));
    }
}
