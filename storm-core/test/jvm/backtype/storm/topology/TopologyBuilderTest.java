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

import org.junit.Test;

import static org.mockito.Mockito.mock;

public class TopologyBuilderTest {
    private final TopologyBuilder builder = new TopologyBuilder();

    @Test(expected = IllegalArgumentException.class)
    public void testSetRichBolt() {
        builder.setBolt("bolt", mock(IRichBolt.class), 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetBasicBolt() {
        builder.setBolt("bolt", mock(IBasicBolt.class), 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetSpout() {
        builder.setSpout("spout", mock(IRichSpout.class), 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAddWorkerHook() {
        builder.addWorkerHook(null);
    }

    // TODO enable if setStateSpout gets implemented
//    @Test(expected = IllegalArgumentException.class)
//    public void testSetStateSpout() {
//        builder.setStateSpout("stateSpout", mock(IRichStateSpout.class), 0);
//    }

}