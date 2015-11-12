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

package backtype.storm.scheduler.resource;

import backtype.storm.Config;
import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.INimbus;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SchedulerAssignmentImpl;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.utils.Time;
import backtype.storm.utils.Utils;
import backtype.storm.validation.ConfigValidation;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class TestResourceAwareScheduler {

    private static final int NUM_SUPS = 20;
    private static final int NUM_WORKERS_PER_SUP = 4;
    private final String TOPOLOGY_SUBMITTER = "jerry";

    private static final Logger LOG = LoggerFactory.getLogger(TestResourceAwareScheduler.class);

    @Test
    public void TestReadInResourceAwareSchedulerUserPools() {

        Map fromFile = Utils.findAndReadConfigFile("user-resource-pools.yaml", false);
        LOG.info("fromFile: {}", fromFile);
        ConfigValidation.validateFields(fromFile);
    }

    @Test
    public void TestTopologySortedInCorrectOrder() {
        INimbus iNimbus = new TestUtilsForResourceAwareScheduler.INimbusTest();
        Map<String, Number> resourceMap = new HashMap<String, Number>();
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 100.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 1024.0);
        Map<String, SupervisorDetails> supMap = TestUtilsForResourceAwareScheduler.genSupervisors(20, 4, resourceMap);
        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());

        config.put(Config.TOPOLOGY_SUBMITTER_USER, TOPOLOGY_SUBMITTER);

        Map<String, Map<String, Number>> resourceUserPool = new HashMap<String, Map<String, Number>>();
        resourceUserPool.put("jerry", new HashMap<String, Number>());
        resourceUserPool.get("jerry").put("cpu", 1000);
        resourceUserPool.get("jerry").put("memory", 8192.0);

        resourceUserPool.put("bobby", new HashMap<String, Number>());
        resourceUserPool.get("bobby").put("cpu", 10000.0);
        resourceUserPool.get("bobby").put("memory", 32768);

        resourceUserPool.put("derek", new HashMap<String, Number>());
        resourceUserPool.get("derek").put("cpu", 5000.0);
        resourceUserPool.get("derek").put("memory", 16384.0);

        config.put(Config.RESOURCE_AWARE_SCHEDULER_USER_POOLS, resourceUserPool);

        TopologyDetails topo1 = TestUtilsForResourceAwareScheduler.getTopology("topo-1", config, 5, 15, 1, 1, Time.currentTimeSecs() - 2, 20);
        TopologyDetails topo2 = TestUtilsForResourceAwareScheduler.getTopology("topo-2", config, 5, 15, 1, 1, Time.currentTimeSecs() - 8, 30);
        TopologyDetails topo3 = TestUtilsForResourceAwareScheduler.getTopology("topo-3", config, 5, 15, 1, 1, Time.currentTimeSecs() - 16, 30);
        TopologyDetails topo4 = TestUtilsForResourceAwareScheduler.getTopology("topo-4", config, 5, 15, 1, 1, Time.currentTimeSecs() - 16, 20);
        TopologyDetails topo5 = TestUtilsForResourceAwareScheduler.getTopology("topo-5", config, 5, 15, 1, 1, Time.currentTimeSecs() - 24, 30);


        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();
        topoMap.put(topo1.getId(), topo1);
        topoMap.put(topo2.getId(), topo2);
        topoMap.put(topo3.getId(), topo3);
        topoMap.put(topo4.getId(), topo4);
        topoMap.put(topo5.getId(), topo5);

        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config);

        Topologies topologies = new Topologies(topoMap);

        ResourceAwareScheduler rs = new ResourceAwareScheduler();

        rs.prepare(config);
        rs.schedule(topologies, cluster);

        Set<TopologyDetails> queue = rs.getUser("jerry").getTopologiesPending();
        Assert.assertEquals("check size", queue.size(), 0);

        queue = rs.getUser("jerry").getTopologiesRunning();

        Iterator<TopologyDetails> itr = queue.iterator();

        TopologyDetails topo = itr.next();
        LOG.info("{} - {}", topo.getName(), queue);
        Assert.assertEquals("check order", topo.getName(), "topo-4");

        topo = itr.next();
        LOG.info("{} - {}", topo.getName(), queue);
        Assert.assertEquals("check order", topo.getName(), "topo-1");

        topo = itr.next();
        LOG.info("{} - {}", topo.getName(), queue);
        Assert.assertEquals("check order", topo.getName(), "topo-5");

        topo = itr.next();
        LOG.info("{} - {}", topo.getName(), queue);
        Assert.assertEquals("check order", topo.getName(), "topo-3");

        topo = itr.next();
        LOG.info("{} - {}", topo.getName(), queue);
        Assert.assertEquals("check order", topo.getName(), "topo-2");

        TopologyDetails topo6 = TestUtilsForResourceAwareScheduler.getTopology("topo-6", config, 5, 15, 1, 1, Time.currentTimeSecs() - 30, 10);
        topoMap.put(topo6.getId(), topo6);

        topologies = new Topologies(topoMap);
        rs = new ResourceAwareScheduler();
        rs.prepare(config);
        rs.schedule(topologies, cluster);

        queue = rs.getUser("jerry").getTopologiesRunning();
        itr = queue.iterator();

        topo = itr.next();
        Assert.assertEquals("check order", topo.getName(), "topo-6");

        topo = itr.next();
        Assert.assertEquals("check order", topo.getName(), "topo-4");

        topo = itr.next();
        Assert.assertEquals("check order", topo.getName(), "topo-1");

        topo = itr.next();
        Assert.assertEquals("check order", topo.getName(), "topo-5");

        topo = itr.next();
        Assert.assertEquals("check order", topo.getName(), "topo-3");

        topo = itr.next();
        Assert.assertEquals("check order", topo.getName(), "topo-2");

        queue = rs.getUser("jerry").getTopologiesPending();
        Assert.assertEquals("check size", queue.size(), 0);
    }

    @Test
    public void TestMultipleUsers() {
        INimbus iNimbus = new TestUtilsForResourceAwareScheduler.INimbusTest();
        Map<String, Number> resourceMap = new HashMap<String, Number>();
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 1000.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 1024.0 * 10);
        Map<String, SupervisorDetails> supMap = TestUtilsForResourceAwareScheduler.genSupervisors(20, 4, resourceMap);
        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        Map<String, Map<String, Number>> resourceUserPool = new HashMap<String, Map<String, Number>>();
        resourceUserPool.put("jerry", new HashMap<String, Number>());
        resourceUserPool.get("jerry").put("cpu", 1000);
        resourceUserPool.get("jerry").put("memory", 8192.0);

        resourceUserPool.put("bobby", new HashMap<String, Number>());
        resourceUserPool.get("bobby").put("cpu", 10000.0);
        resourceUserPool.get("bobby").put("memory", 32768);

        resourceUserPool.put("derek", new HashMap<String, Number>());
        resourceUserPool.get("derek").put("cpu", 5000.0);
        resourceUserPool.get("derek").put("memory", 16384.0);

        config.put(Config.RESOURCE_AWARE_SCHEDULER_USER_POOLS, resourceUserPool);
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "jerry");

        TopologyDetails topo1 = TestUtilsForResourceAwareScheduler.getTopology("topo-1", config, 5, 15, 1, 1, Time.currentTimeSecs() - 2, 20);
        TopologyDetails topo2 = TestUtilsForResourceAwareScheduler.getTopology("topo-2", config, 5, 15, 1, 1, Time.currentTimeSecs() - 8, 30);
        TopologyDetails topo3 = TestUtilsForResourceAwareScheduler.getTopology("topo-3", config, 5, 15, 1, 1, Time.currentTimeSecs() - 16, 30);
        TopologyDetails topo4 = TestUtilsForResourceAwareScheduler.getTopology("topo-4", config, 5, 15, 1, 1, Time.currentTimeSecs() - 16, 20);
        TopologyDetails topo5 = TestUtilsForResourceAwareScheduler.getTopology("topo-5", config, 5, 15, 1, 1, Time.currentTimeSecs() - 24, 30);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "bobby");

        TopologyDetails topo6 = TestUtilsForResourceAwareScheduler.getTopology("topo-6", config, 5, 15, 1, 1, Time.currentTimeSecs() - 2, 20);
        TopologyDetails topo7 = TestUtilsForResourceAwareScheduler.getTopology("topo-7", config, 5, 15, 1, 1, Time.currentTimeSecs() - 8, 30);
        TopologyDetails topo8 = TestUtilsForResourceAwareScheduler.getTopology("topo-8", config, 5, 15, 1, 1, Time.currentTimeSecs() - 16, 30);
        TopologyDetails topo9 = TestUtilsForResourceAwareScheduler.getTopology("topo-9", config, 5, 15, 1, 1, Time.currentTimeSecs() - 16, 20);
        TopologyDetails topo10 = TestUtilsForResourceAwareScheduler.getTopology("topo-10", config, 5, 15, 1, 1, Time.currentTimeSecs() - 24, 30);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "derek");

        TopologyDetails topo11 = TestUtilsForResourceAwareScheduler.getTopology("topo-11", config, 5, 15, 1, 1, Time.currentTimeSecs() - 2, 20);
        TopologyDetails topo12 = TestUtilsForResourceAwareScheduler.getTopology("topo-12", config, 5, 15, 1, 1, Time.currentTimeSecs() - 8, 30);
        TopologyDetails topo13 = TestUtilsForResourceAwareScheduler.getTopology("topo-13", config, 5, 15, 1, 1, Time.currentTimeSecs() - 16, 30);
        TopologyDetails topo14 = TestUtilsForResourceAwareScheduler.getTopology("topo-14", config, 5, 15, 1, 1, Time.currentTimeSecs() - 16, 20);
        TopologyDetails topo15 = TestUtilsForResourceAwareScheduler.getTopology("topo-15", config, 5, 15, 1, 1, Time.currentTimeSecs() - 24, 30);

        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();
        topoMap.put(topo1.getId(), topo1);
        topoMap.put(topo2.getId(), topo2);
        topoMap.put(topo3.getId(), topo3);
        topoMap.put(topo4.getId(), topo4);
        topoMap.put(topo5.getId(), topo5);
        topoMap.put(topo6.getId(), topo6);
        topoMap.put(topo7.getId(), topo7);
        topoMap.put(topo8.getId(), topo8);
        topoMap.put(topo9.getId(), topo9);
        topoMap.put(topo10.getId(), topo10);
        topoMap.put(topo11.getId(), topo11);
        topoMap.put(topo12.getId(), topo12);
        topoMap.put(topo13.getId(), topo13);
        topoMap.put(topo14.getId(), topo14);
        topoMap.put(topo15.getId(), topo15);

        Topologies topologies = new Topologies(topoMap);

        ResourceAwareScheduler rs = new ResourceAwareScheduler();

        rs.prepare(config);
        rs.schedule(topologies, cluster);

        for (TopologyDetails topo : topoMap.values()) {
            Assert.assertEquals(cluster.getStatusMap().get(topo.getId()), "Fully Scheduled");
        }

        for (User user : rs.getUserMap().values()) {
            Assert.assertEquals(user.getTopologiesPending().size(), 0);
            Assert.assertEquals(user.getTopologiesRunning().size(), 5);
        }
    }

    @Test
    public void testHandlingClusterSubscription() {
        INimbus iNimbus = new TestUtilsForResourceAwareScheduler.INimbusTest();
        Map<String, Number> resourceMap = new HashMap<String, Number>();
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 200.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 1024.0 * 10);
        Map<String, SupervisorDetails> supMap = TestUtilsForResourceAwareScheduler.genSupervisors(1, 4, resourceMap);
        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        Map<String, Map<String, Number>> resourceUserPool = new HashMap<String, Map<String, Number>>();
        resourceUserPool.put("jerry", new HashMap<String, Number>());
        resourceUserPool.get("jerry").put("cpu", 1000);
        resourceUserPool.get("jerry").put("memory", 8192.0);

        resourceUserPool.put("bobby", new HashMap<String, Number>());
        resourceUserPool.get("bobby").put("cpu", 10000.0);
        resourceUserPool.get("bobby").put("memory", 32768);

        resourceUserPool.put("derek", new HashMap<String, Number>());
        resourceUserPool.get("derek").put("cpu", 5000.0);
        resourceUserPool.get("derek").put("memory", 16384.0);

        config.put(Config.RESOURCE_AWARE_SCHEDULER_USER_POOLS, resourceUserPool);
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "jerry");

        TopologyDetails topo1 = TestUtilsForResourceAwareScheduler.getTopology("topo-1", config, 5, 15, 1, 1, Time.currentTimeSecs() - 2, 20);
        TopologyDetails topo2 = TestUtilsForResourceAwareScheduler.getTopology("topo-2", config, 5, 15, 1, 1, Time.currentTimeSecs() - 8, 30);

        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();
        topoMap.put(topo1.getId(), topo1);
        topoMap.put(topo2.getId(), topo2);

        Topologies topologies = new Topologies(topoMap);

        ResourceAwareScheduler rs = new ResourceAwareScheduler();

        rs.prepare(config);
        rs.schedule(topologies, cluster);

        int fullyScheduled = 0;
        for (TopologyDetails topo : topoMap.values()) {
            if(cluster.getStatusMap().get(topo.getId()).equals("Fully Scheduled")) {
                fullyScheduled++;
            }
        }
        Assert.assertEquals("# of Fully scheduled", 1, fullyScheduled);
        Assert.assertEquals("# of topologies schedule attempted", 1, rs.getUser("jerry").getTopologiesAttempted().size());
        Assert.assertEquals("# of topologies running", 1, rs.getUser("jerry").getTopologiesRunning().size());
        Assert.assertEquals("# of topologies schedule pending", 0, rs.getUser("jerry").getTopologiesPending().size());
    }
}
