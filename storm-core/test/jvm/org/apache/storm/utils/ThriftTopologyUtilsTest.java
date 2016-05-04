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
package org.apache.storm.utils;

import org.apache.storm.generated.*;
import org.apache.storm.hooks.BaseWorkerHook;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Set;

public class ThriftTopologyUtilsTest extends TestCase {
    @Test
    public void testIsWorkerHook() {
        Assert.assertEquals(false, ThriftTopologyUtils.isWorkerHook(StormTopology._Fields.BOLTS));
        Assert.assertEquals(false, ThriftTopologyUtils.isWorkerHook(StormTopology._Fields.SPOUTS));
        Assert.assertEquals(false, ThriftTopologyUtils.isWorkerHook(StormTopology._Fields.STATE_SPOUTS));
        Assert.assertEquals(true, ThriftTopologyUtils.isWorkerHook(StormTopology._Fields.WORKER_HOOKS));
    }

    @Test
    public void testGetComponentIdsWithWorkerHook() {
        StormTopology stormTopology = genereateStormTopology(true);
        Set<String> componentIds = ThriftTopologyUtils.getComponentIds(stormTopology);
        Assert.assertEquals(
                "We expect to get the IDs of the components sans the Worker Hook",
                ImmutableSet.of("bolt-1", "spout-1"),
                componentIds);
    }

    @Test
    public void testGetComponentIdsWithoutWorkerHook() {
        StormTopology stormTopology = genereateStormTopology(false);
        Set<String> componentIds = ThriftTopologyUtils.getComponentIds(stormTopology);
        Assert.assertEquals(
                "We expect to get the IDs of the components sans the Worker Hook",
                ImmutableSet.of("bolt-1", "spout-1"),
                componentIds);
    }

    @Test
    public void testGetComponentCommonWithWorkerHook() {
        StormTopology stormTopology = genereateStormTopology(true);
        ComponentCommon componentCommon = ThriftTopologyUtils.getComponentCommon(stormTopology, "bolt-1");
        Assert.assertEquals(
                "We expect to get bolt-1's common",
                new Bolt().get_common(),
                componentCommon);
    }

    @Test
    public void testGetComponentCommonWithoutWorkerHook() {
        StormTopology stormTopology = genereateStormTopology(false);
        ComponentCommon componentCommon = ThriftTopologyUtils.getComponentCommon(stormTopology, "bolt-1");
        Assert.assertEquals(
                "We expect to get bolt-1's common",
                new Bolt().get_common(),
                componentCommon);
    }

    private StormTopology genereateStormTopology(boolean withWorkerHook) {
        ImmutableMap<String,SpoutSpec> spouts = ImmutableMap.of("spout-1", new SpoutSpec());
        ImmutableMap<String,Bolt> bolts = ImmutableMap.of("bolt-1", new Bolt());
        ImmutableMap<String,StateSpoutSpec> state_spouts = ImmutableMap.of();

        StormTopology stormTopology = new StormTopology(spouts, bolts, state_spouts);

        if(withWorkerHook) {
            BaseWorkerHook workerHook = new BaseWorkerHook();
            stormTopology.add_to_worker_hooks(ByteBuffer.wrap(Utils.javaSerialize(workerHook)));
        }

        return stormTopology;
    }
}
