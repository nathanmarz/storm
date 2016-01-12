/**
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

package org.apache.storm.scheduler.resource;

import org.apache.storm.Config;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TestUser {

    private static final Logger LOG = LoggerFactory.getLogger(TestUser.class);

    @Test
    public void testAddTopologyToPendingQueue() {

        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());

        List<TopologyDetails> topos = TestUtilsForResourceAwareScheduler.getListOfTopologies(config);
        User user1 = new User("user1");

        for (TopologyDetails topo : topos) {
            user1.addTopologyToPendingQueue(topo);
        }

        Assert.assertTrue(user1.getTopologiesPending().size() == topos.size());

        List<String> correctOrder = TestUtilsForResourceAwareScheduler.getListOfTopologiesCorrectOrder();
        Iterator<String> itr = correctOrder.iterator();
        for (TopologyDetails topo : user1.getTopologiesPending()) {
            Assert.assertEquals("check order", topo.getName(), itr.next());
        }
    }

    @Test
    public void testMoveTopoFromPendingToRunning() {

        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());

        List<TopologyDetails> topos = TestUtilsForResourceAwareScheduler.getListOfTopologies(config);
        User user1 = new User("user1");

        for (TopologyDetails topo : topos) {
            user1.addTopologyToPendingQueue(topo);
        }

        int counter = 1;
        for (TopologyDetails topo : topos) {
            user1.moveTopoFromPendingToRunning(topo);
            Assert.assertEquals("check correct size", (topos.size() - counter), user1.getTopologiesPending().size());
            Assert.assertEquals("check correct size", counter, user1.getTopologiesRunning().size());
            counter++;
        }
    }

    @Test
    public void testResourcePoolUtilization() {

        Double cpuGuarantee = 400.0;
        Double memoryGuarantee = 1000.0;
        Map<String, Double> resourceGuaranteeMap = new HashMap<String, Double>();
        resourceGuaranteeMap.put("cpu", cpuGuarantee);
        resourceGuaranteeMap.put("memory", memoryGuarantee);

        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 100);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 200);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 200);

        TopologyDetails topo1 = TestUtilsForResourceAwareScheduler.getTopology("topo-1", config, 1, 1, 2, 1, Time.currentTimeSecs() - 24, 9);

        User user1 = new User("user1", resourceGuaranteeMap);

        user1.addTopologyToRunningQueue(topo1);

        Assert.assertEquals("check cpu resource guarantee", cpuGuarantee, user1.getCPUResourceGuaranteed(), 0.001);
        Assert.assertEquals("check memory resource guarantee", memoryGuarantee, user1.getMemoryResourceGuaranteed(), 0.001);

        Assert.assertEquals("check cpu resource pool utilization", ((100.0 * 3.0) / cpuGuarantee), user1.getCPUResourcePoolUtilization(), 0.001);
        Assert.assertEquals("check memory resource pool utilization", ((200.0 + 200.0) * 3.0) / memoryGuarantee, user1.getMemoryResourcePoolUtilization(), 0.001);
    }

}
