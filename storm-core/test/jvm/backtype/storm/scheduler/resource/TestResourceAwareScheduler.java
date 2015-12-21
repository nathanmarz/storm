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
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.INimbus;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.SchedulerAssignmentImpl;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.utils.Utils;
import backtype.storm.validation.ConfigValidation;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class TestResourceAwareScheduler {

    private final String TOPOLOGY_SUBMITTER = "jerry";

    private static final Logger LOG = LoggerFactory.getLogger(TestResourceAwareScheduler.class);

    private static int currentTime = 1450418597;

    @Test
    public void TestReadInResourceAwareSchedulerUserPools() {
        Map fromFile = Utils.findAndReadConfigFile("user-resource-pools.yaml", false);
        LOG.info("fromFile: {}", fromFile);
        ConfigValidation.validateFields(fromFile);
    }

    @Test
    public void TestSubmitUsersWithNoGuarantees() {
        INimbus iNimbus = new TestUtilsForResourceAwareScheduler.INimbusTest();
        Map<String, Number> resourceMap = new HashMap<String, Number>();
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 100.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 1000.0);
        Map<String, SupervisorDetails> supMap = TestUtilsForResourceAwareScheduler.genSupervisors(4, 4, resourceMap);
        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY, backtype.storm.scheduler.resource.strategies.eviction.DefaultEvictionStrategy.class.getName());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, backtype.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy.class.getName());
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, backtype.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy.class.getName());
        config.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 100.0);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 500);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 500);

        Map<String, Map<String, Number>> resourceUserPool = new HashMap<String, Map<String, Number>>();
        resourceUserPool.put("jerry", new HashMap<String, Number>());
        resourceUserPool.get("jerry").put("cpu", 200.0);
        resourceUserPool.get("jerry").put("memory", 2000.0);

        config.put(Config.RESOURCE_AWARE_SCHEDULER_USER_POOLS, resourceUserPool);
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "jerry");

        TopologyDetails topo1 = TestUtilsForResourceAwareScheduler.getTopology("topo-1", config, 1, 0, 1, 0, currentTime - 2, 10);
        TopologyDetails topo2 = TestUtilsForResourceAwareScheduler.getTopology("topo-2", config, 1, 0, 1, 0, currentTime - 2, 20);
        TopologyDetails topo3 = TestUtilsForResourceAwareScheduler.getTopology("topo-3", config, 1, 0, 1, 0, currentTime - 2, 20);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "bobby");

        TopologyDetails topo4 = TestUtilsForResourceAwareScheduler.getTopology("topo-4", config, 1, 0, 1, 0, currentTime - 2, 10);
        TopologyDetails topo5 = TestUtilsForResourceAwareScheduler.getTopology("topo-5", config, 1, 0, 1, 0, currentTime - 2, 20);

        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();
        topoMap.put(topo1.getId(), topo1);
        topoMap.put(topo2.getId(), topo2);
        topoMap.put(topo3.getId(), topo3);
        topoMap.put(topo4.getId(), topo4);
        topoMap.put(topo5.getId(), topo5);

        Topologies topologies = new Topologies(topoMap);

        ResourceAwareScheduler rs = new ResourceAwareScheduler();

        rs.prepare(config);
        rs.schedule(topologies, cluster);

        for (TopologyDetails topo : rs.getUser("jerry").getTopologiesRunning()) {
            Assert.assertTrue("assert topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 3, rs.getUser("jerry").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("jerry").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("jerry").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("jerry").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("bobby").getTopologiesRunning()) {
            Assert.assertTrue("assert topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 1, rs.getUser("bobby").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("bobby").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 1, rs.getUser("bobby").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("bobby").getTopologiesInvalid().size());
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
        config.put(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY, backtype.storm.scheduler.resource.strategies.eviction.DefaultEvictionStrategy.class.getName());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, backtype.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy.class.getName());
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, backtype.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy.class.getName());

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

        TopologyDetails topo1 = TestUtilsForResourceAwareScheduler.getTopology("topo-1", config, 5, 15, 1, 1, currentTime - 2, 20);
        TopologyDetails topo2 = TestUtilsForResourceAwareScheduler.getTopology("topo-2", config, 5, 15, 1, 1, currentTime - 8, 30);
        TopologyDetails topo3 = TestUtilsForResourceAwareScheduler.getTopology("topo-3", config, 5, 15, 1, 1, currentTime - 16, 30);
        TopologyDetails topo4 = TestUtilsForResourceAwareScheduler.getTopology("topo-4", config, 5, 15, 1, 1, currentTime - 16, 20);
        TopologyDetails topo5 = TestUtilsForResourceAwareScheduler.getTopology("topo-5", config, 5, 15, 1, 1, currentTime - 24, 30);

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

        TopologyDetails topo6 = TestUtilsForResourceAwareScheduler.getTopology("topo-6", config, 5, 15, 1, 1, currentTime - 30, 10);
        topoMap.put(topo6.getId(), topo6);

        topologies = new Topologies(topoMap);
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
        config.put(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY, backtype.storm.scheduler.resource.strategies.eviction.DefaultEvictionStrategy.class.getName());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, backtype.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy.class.getName());
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, backtype.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy.class.getName());
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

        TopologyDetails topo1 = TestUtilsForResourceAwareScheduler.getTopology("topo-1", config, 5, 15, 1, 1, currentTime - 2, 20);
        TopologyDetails topo2 = TestUtilsForResourceAwareScheduler.getTopology("topo-2", config, 5, 15, 1, 1, currentTime - 8, 29);
        TopologyDetails topo3 = TestUtilsForResourceAwareScheduler.getTopology("topo-3", config, 5, 15, 1, 1, currentTime - 16, 29);
        TopologyDetails topo4 = TestUtilsForResourceAwareScheduler.getTopology("topo-4", config, 5, 15, 1, 1, currentTime - 16, 20);
        TopologyDetails topo5 = TestUtilsForResourceAwareScheduler.getTopology("topo-5", config, 5, 15, 1, 1, currentTime - 24, 29);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "bobby");

        TopologyDetails topo6 = TestUtilsForResourceAwareScheduler.getTopology("topo-6", config, 5, 15, 1, 1, currentTime - 2, 20);
        TopologyDetails topo7 = TestUtilsForResourceAwareScheduler.getTopology("topo-7", config, 5, 15, 1, 1, currentTime - 8, 29);
        TopologyDetails topo8 = TestUtilsForResourceAwareScheduler.getTopology("topo-8", config, 5, 15, 1, 1, currentTime - 16, 29);
        TopologyDetails topo9 = TestUtilsForResourceAwareScheduler.getTopology("topo-9", config, 5, 15, 1, 1, currentTime - 16, 20);
        TopologyDetails topo10 = TestUtilsForResourceAwareScheduler.getTopology("topo-10", config, 5, 15, 1, 1, currentTime - 24, 29);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "derek");

        TopologyDetails topo11 = TestUtilsForResourceAwareScheduler.getTopology("topo-11", config, 5, 15, 1, 1, currentTime - 2, 20);
        TopologyDetails topo12 = TestUtilsForResourceAwareScheduler.getTopology("topo-12", config, 5, 15, 1, 1, currentTime - 8, 29);
        TopologyDetails topo13 = TestUtilsForResourceAwareScheduler.getTopology("topo-13", config, 5, 15, 1, 1, currentTime - 16, 29);
        TopologyDetails topo14 = TestUtilsForResourceAwareScheduler.getTopology("topo-14", config, 5, 15, 1, 1, currentTime - 16, 20);
        TopologyDetails topo15 = TestUtilsForResourceAwareScheduler.getTopology("topo-15", config, 5, 15, 1, 1, currentTime - 24, 29);

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
            Assert.assertTrue(TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
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
        config.put(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY, backtype.storm.scheduler.resource.strategies.eviction.DefaultEvictionStrategy.class.getName());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, backtype.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy.class.getName());
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, backtype.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy.class.getName());
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

        TopologyDetails topo1 = TestUtilsForResourceAwareScheduler.getTopology("topo-1", config, 5, 15, 1, 1, currentTime - 2, 20);
        TopologyDetails topo2 = TestUtilsForResourceAwareScheduler.getTopology("topo-2", config, 5, 15, 1, 1, currentTime - 8, 29);

        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();
        topoMap.put(topo1.getId(), topo1);
        topoMap.put(topo2.getId(), topo2);

        Topologies topologies = new Topologies(topoMap);

        ResourceAwareScheduler rs = new ResourceAwareScheduler();

        rs.prepare(config);
        rs.schedule(topologies, cluster);

        int fullyScheduled = 0;
        for (TopologyDetails topo : topoMap.values()) {
            if (TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId()))) {
                fullyScheduled++;
            }
        }
        Assert.assertEquals("# of Fully scheduled", 1, fullyScheduled);
        Assert.assertEquals("# of topologies schedule attempted", 1, rs.getUser("jerry").getTopologiesAttempted().size());
        Assert.assertEquals("# of topologies running", 1, rs.getUser("jerry").getTopologiesRunning().size());
        Assert.assertEquals("# of topologies schedule pending", 0, rs.getUser("jerry").getTopologiesPending().size());
    }

    /**
     * The resources in the cluster are limited. In the first round of scheduling, all resources in the cluster is used.
     * User jerry submits another toploogy.  Since user jerry has his resource guarantees satisfied, and user bobby
     * has exceeded his resource guarantee, topo-3 from user bobby should be evicted.
     */
    @Test
    public void testEviction() {
        INimbus iNimbus = new TestUtilsForResourceAwareScheduler.INimbusTest();
        Map<String, Number> resourceMap = new HashMap<String, Number>();
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 100.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 1000.0);
        Map<String, SupervisorDetails> supMap = TestUtilsForResourceAwareScheduler.genSupervisors(4, 4, resourceMap);
        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY, backtype.storm.scheduler.resource.strategies.eviction.DefaultEvictionStrategy.class.getName());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, backtype.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy.class.getName());
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, backtype.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy.class.getName());
        config.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 100.0);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 500);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 500);
        Map<String, Map<String, Number>> resourceUserPool = new HashMap<String, Map<String, Number>>();
        resourceUserPool.put("jerry", new HashMap<String, Number>());
        resourceUserPool.get("jerry").put("cpu", 200.0);
        resourceUserPool.get("jerry").put("memory", 2000.0);

        resourceUserPool.put("bobby", new HashMap<String, Number>());
        resourceUserPool.get("bobby").put("cpu", 100.0);
        resourceUserPool.get("bobby").put("memory", 1000.0);

        resourceUserPool.put("derek", new HashMap<String, Number>());
        resourceUserPool.get("derek").put("cpu", 200.0);
        resourceUserPool.get("derek").put("memory", 2000.0);

        config.put(Config.RESOURCE_AWARE_SCHEDULER_USER_POOLS, resourceUserPool);
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "jerry");

        TopologyDetails topo1 = TestUtilsForResourceAwareScheduler.getTopology("topo-1", config, 1, 0, 1, 0, currentTime - 2, 10);
        TopologyDetails topo6 = TestUtilsForResourceAwareScheduler.getTopology("topo-6", config, 1, 0, 1, 0, currentTime - 2, 20);


        config.put(Config.TOPOLOGY_SUBMITTER_USER, "bobby");

        TopologyDetails topo2 = TestUtilsForResourceAwareScheduler.getTopology("topo-2", config, 1, 0, 1, 0, currentTime - 2, 10);
        TopologyDetails topo3 = TestUtilsForResourceAwareScheduler.getTopology("topo-3", config, 1, 0, 1, 0, currentTime - 2, 20);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "derek");

        TopologyDetails topo4 = TestUtilsForResourceAwareScheduler.getTopology("topo-4", config, 1, 0, 1, 0, currentTime - 2, 29);

        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();
        topoMap.put(topo1.getId(), topo1);
        topoMap.put(topo2.getId(), topo2);
        topoMap.put(topo3.getId(), topo3);
        topoMap.put(topo4.getId(), topo4);

        Topologies topologies = new Topologies(topoMap);

        ResourceAwareScheduler rs = new ResourceAwareScheduler();

        rs.prepare(config);
        rs.schedule(topologies, cluster);

        for (TopologyDetails topo : rs.getUser("jerry").getTopologiesRunning()) {
            Assert.assertTrue("assert topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 1, rs.getUser("jerry").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("jerry").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("jerry").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("jerry").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("derek").getTopologiesRunning()) {
            Assert.assertTrue("assert topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 1, rs.getUser("derek").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("derek").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("derek").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("derek").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("bobby").getTopologiesRunning()) {
            Assert.assertTrue("assert topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("bobby").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("bobby").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("bobby").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("bobby").getTopologiesInvalid().size());

        //user jerry submits another topology
        topoMap.put(topo6.getId(), topo6);
        topologies = new Topologies(topoMap);

        rs.schedule(topologies, cluster);

        for (TopologyDetails topo : rs.getUser("jerry").getTopologiesRunning()) {
            Assert.assertTrue("assert topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("jerry").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("jerry").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("jerry").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("jerry").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("derek").getTopologiesRunning()) {
            Assert.assertTrue("assert topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 1, rs.getUser("derek").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("derek").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("derek").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("derek").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("bobby").getTopologiesRunning()) {
            Assert.assertTrue("assert topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 1, rs.getUser("bobby").getTopologiesRunning().size());

        for (TopologyDetails topo : rs.getUser("bobby").getTopologiesAttempted()) {
            Assert.assertFalse("assert topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of attempted topologies", 1, rs.getUser("bobby").getTopologiesAttempted().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("bobby").getTopologiesPending().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("bobby").getTopologiesInvalid().size());
        Assert.assertEquals("correct topology to evict", "topo-3", rs.getUser("bobby").getTopologiesAttempted().iterator().next().getName());
    }

    @Test
    public void TestEvictMultipleTopologies() {
        INimbus iNimbus = new TestUtilsForResourceAwareScheduler.INimbusTest();
        Map<String, Number> resourceMap = new HashMap<String, Number>();
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 100.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 1000.0);
        Map<String, SupervisorDetails> supMap = TestUtilsForResourceAwareScheduler.genSupervisors(4, 4, resourceMap);
        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY, backtype.storm.scheduler.resource.strategies.eviction.DefaultEvictionStrategy.class.getName());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, backtype.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy.class.getName());
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, backtype.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy.class.getName());
        config.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 100.0);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 500);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 500);
        Map<String, Map<String, Number>> resourceUserPool = new HashMap<String, Map<String, Number>>();
        resourceUserPool.put("jerry", new HashMap<String, Number>());
        resourceUserPool.get("jerry").put("cpu", 200.0);
        resourceUserPool.get("jerry").put("memory", 2000.0);

        resourceUserPool.put("derek", new HashMap<String, Number>());
        resourceUserPool.get("derek").put("cpu", 100.0);
        resourceUserPool.get("derek").put("memory", 1000.0);

        config.put(Config.RESOURCE_AWARE_SCHEDULER_USER_POOLS, resourceUserPool);
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "jerry");

        TopologyDetails topo1 = TestUtilsForResourceAwareScheduler.getTopology("topo-1", config, 2, 0, 1, 0, currentTime - 2, 10);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "bobby");

        TopologyDetails topo2 = TestUtilsForResourceAwareScheduler.getTopology("topo-2", config, 1, 0, 1, 0, currentTime - 2, 10);
        TopologyDetails topo3 = TestUtilsForResourceAwareScheduler.getTopology("topo-3", config, 1, 0, 1, 0, currentTime - 2, 20);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "derek");

        TopologyDetails topo4 = TestUtilsForResourceAwareScheduler.getTopology("topo-4", config, 1, 0, 1, 0, currentTime - 2, 29);
        TopologyDetails topo5 = TestUtilsForResourceAwareScheduler.getTopology("topo-5", config, 1, 0, 1, 0, currentTime - 2, 29);

        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();
        topoMap.put(topo2.getId(), topo2);
        topoMap.put(topo3.getId(), topo3);
        topoMap.put(topo4.getId(), topo4);
        topoMap.put(topo5.getId(), topo5);

        Topologies topologies = new Topologies(topoMap);

        ResourceAwareScheduler rs = new ResourceAwareScheduler();

        rs.prepare(config);
        rs.schedule(topologies, cluster);

        for (TopologyDetails topo : rs.getUser("derek").getTopologiesRunning()) {
            Assert.assertTrue("assert topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("derek").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("derek").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("derek").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("derek").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("bobby").getTopologiesRunning()) {
            Assert.assertTrue("assert topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("bobby").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("bobby").getTopologiesPending().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("bobby").getTopologiesInvalid().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("bobby").getTopologiesAttempted().size());

        //user jerry submits another topology
        topoMap.put(topo1.getId(), topo1);
        topologies = new Topologies(topoMap);
        rs.schedule(topologies, cluster);

        for (TopologyDetails topo : rs.getUser("jerry").getTopologiesRunning()) {
            Assert.assertTrue("assert topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 1, rs.getUser("jerry").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("jerry").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("jerry").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("jerry").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("derek").getTopologiesRunning()) {
            Assert.assertTrue("assert topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("derek").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("derek").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("derek").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("derek").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("bobby").getTopologiesAttempted()) {
            Assert.assertFalse("assert topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of attempted topologies", 2, rs.getUser("bobby").getTopologiesAttempted().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("bobby").getTopologiesPending().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("bobby").getTopologiesInvalid().size());
        Assert.assertEquals("# of running topologies", 0, rs.getUser("bobby").getTopologiesRunning().size());
    }

    /**
     * Eviction order:
     * topo-3: since user bobby don't have any resource guarantees and topo-3 is the lowest priority for user bobby
     * topo-2: since user bobby don't have any resource guarantees and topo-2 is the next lowest priority for user bobby
     * topo-5: since user derek has exceeded his resource guarantee while user jerry has not.  topo-5 and topo-4 has the same priority
     * but topo-4 was submitted earlier thus we choose that one to evict
     */
    @Test
    public void TestEvictMultipleTopologiesFromMultipleUsersInCorrectOrder() {
        INimbus iNimbus = new TestUtilsForResourceAwareScheduler.INimbusTest();
        Map<String, Number> resourceMap = new HashMap<String, Number>();
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 100.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 1000.0);
        Map<String, SupervisorDetails> supMap = TestUtilsForResourceAwareScheduler.genSupervisors(4, 4, resourceMap);
        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY, backtype.storm.scheduler.resource.strategies.eviction.DefaultEvictionStrategy.class.getName());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, backtype.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy.class.getName());
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, backtype.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy.class.getName());
        config.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 100.0);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 500);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 500);
        Map<String, Map<String, Number>> resourceUserPool = new HashMap<String, Map<String, Number>>();
        resourceUserPool.put("jerry", new HashMap<String, Number>());
        resourceUserPool.get("jerry").put("cpu", 300.0);
        resourceUserPool.get("jerry").put("memory", 3000.0);

        resourceUserPool.put("derek", new HashMap<String, Number>());
        resourceUserPool.get("derek").put("cpu", 100.0);
        resourceUserPool.get("derek").put("memory", 1000.0);

        config.put(Config.RESOURCE_AWARE_SCHEDULER_USER_POOLS, resourceUserPool);
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "jerry");

        TopologyDetails topo1 = TestUtilsForResourceAwareScheduler.getTopology("topo-1", config, 1, 0, 1, 0, currentTime - 2, 10);
        TopologyDetails topo6 = TestUtilsForResourceAwareScheduler.getTopology("topo-6", config, 1, 0, 1, 0, currentTime - 2, 10);
        TopologyDetails topo7 = TestUtilsForResourceAwareScheduler.getTopology("topo-7", config, 1, 0, 1, 0, currentTime - 2, 10);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "bobby");

        TopologyDetails topo2 = TestUtilsForResourceAwareScheduler.getTopology("topo-2", config, 1, 0, 1, 0, currentTime - 2, 10);
        TopologyDetails topo3 = TestUtilsForResourceAwareScheduler.getTopology("topo-3", config, 1, 0, 1, 0, currentTime - 2, 20);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "derek");

        TopologyDetails topo4 = TestUtilsForResourceAwareScheduler.getTopology("topo-4", config, 1, 0, 1, 0, currentTime - 2, 29);
        TopologyDetails topo5 = TestUtilsForResourceAwareScheduler.getTopology("topo-5", config, 1, 0, 1, 0, currentTime - 15, 29);

        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();
        topoMap.put(topo2.getId(), topo2);
        topoMap.put(topo3.getId(), topo3);
        topoMap.put(topo4.getId(), topo4);
        topoMap.put(topo5.getId(), topo5);

        Topologies topologies = new Topologies(topoMap);

        ResourceAwareScheduler rs = new ResourceAwareScheduler();

        rs.prepare(config);
        rs.schedule(topologies, cluster);

        for (TopologyDetails topo : rs.getUser("derek").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("derek").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("derek").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("derek").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("derek").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("bobby").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("bobby").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("bobby").getTopologiesPending().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("bobby").getTopologiesInvalid().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("bobby").getTopologiesAttempted().size());

        //user jerry submits another topology
        topoMap.put(topo1.getId(), topo1);
        topologies = new Topologies(topoMap);
        rs.schedule(topologies, cluster);

        for (TopologyDetails topo : rs.getUser("jerry").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 1, rs.getUser("jerry").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("jerry").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("jerry").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("jerry").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("derek").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("derek").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("derek").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("derek").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("derek").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("bobby").getTopologiesAttempted()) {
            Assert.assertFalse("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        for (TopologyDetails topo : rs.getUser("bobby").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of attempted topologies", 1, rs.getUser("bobby").getTopologiesAttempted().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("bobby").getTopologiesPending().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("bobby").getTopologiesInvalid().size());
        Assert.assertEquals("# of running topologies", 1, rs.getUser("bobby").getTopologiesRunning().size());
        Assert.assertEquals("correct topology to evict", rs.getUser("bobby").getTopologiesAttempted().iterator().next().getName(), "topo-3");

        topoMap.put(topo6.getId(), topo6);
        topologies = new Topologies(topoMap);
        rs.schedule(topologies, cluster);

        for (TopologyDetails topo : rs.getUser("jerry").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("jerry").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("jerry").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("jerry").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("jerry").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("derek").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("derek").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("derek").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("derek").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("derek").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("bobby").getTopologiesAttempted()) {
            Assert.assertFalse("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of attempted topologies", 2, rs.getUser("bobby").getTopologiesAttempted().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("bobby").getTopologiesPending().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("bobby").getTopologiesInvalid().size());
        Assert.assertEquals("# of running topologies", 0, rs.getUser("bobby").getTopologiesRunning().size());

        Assert.assertTrue("correct topology to evict", TestUtilsForResourceAwareScheduler.findTopologyInSetFromName("topo-2", rs.getUser("bobby").getTopologiesAttempted()) != null);
        Assert.assertTrue("correct topology to evict", TestUtilsForResourceAwareScheduler.findTopologyInSetFromName("topo-3", rs.getUser("bobby").getTopologiesAttempted()) != null);

        topoMap.put(topo7.getId(), topo7);
        topologies = new Topologies(topoMap);
        rs.schedule(topologies, cluster);

        for (TopologyDetails topo : rs.getUser("jerry").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 3, rs.getUser("jerry").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("jerry").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("jerry").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("jerry").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("derek").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        for (TopologyDetails topo : rs.getUser("derek").getTopologiesAttempted()) {
            Assert.assertFalse("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 1, rs.getUser("derek").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("derek").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 1, rs.getUser("derek").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("derek").getTopologiesInvalid().size());
        Assert.assertEquals("correct topology to evict", rs.getUser("derek").getTopologiesAttempted().iterator().next().getName(), "topo-4");

        for (TopologyDetails topo : rs.getUser("bobby").getTopologiesAttempted()) {
            Assert.assertFalse("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of attempted topologies", 2, rs.getUser("bobby").getTopologiesAttempted().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("bobby").getTopologiesPending().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("bobby").getTopologiesInvalid().size());
        Assert.assertEquals("# of running topologies", 0, rs.getUser("bobby").getTopologiesRunning().size());

        Assert.assertTrue("correct topology to evict", TestUtilsForResourceAwareScheduler.findTopologyInSetFromName("topo-2", rs.getUser("bobby").getTopologiesAttempted()) != null);
        Assert.assertTrue("correct topology to evict", TestUtilsForResourceAwareScheduler.findTopologyInSetFromName("topo-3", rs.getUser("bobby").getTopologiesAttempted()) != null);
    }

    /**
     * If topologies from other users cannot be evicted to make space
     * check if there is a topology with lower priority that can be evicted from the current user
     */
    @Test
    public void TestEvictTopologyFromItself() {
        INimbus iNimbus = new TestUtilsForResourceAwareScheduler.INimbusTest();
        Map<String, Number> resourceMap = new HashMap<String, Number>();
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 100.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 1000.0);
        Map<String, SupervisorDetails> supMap = TestUtilsForResourceAwareScheduler.genSupervisors(4, 4, resourceMap);
        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY, backtype.storm.scheduler.resource.strategies.eviction.DefaultEvictionStrategy.class.getName());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, backtype.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy.class.getName());
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, backtype.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy.class.getName());
        config.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 100.0);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 500);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 500);
        Map<String, Map<String, Number>> resourceUserPool = new HashMap<String, Map<String, Number>>();
        resourceUserPool.put("jerry", new HashMap<String, Number>());
        resourceUserPool.get("jerry").put("cpu", 200.0);
        resourceUserPool.get("jerry").put("memory", 2000.0);

        resourceUserPool.put("bobby", new HashMap<String, Number>());
        resourceUserPool.get("bobby").put("cpu", 100.0);
        resourceUserPool.get("bobby").put("memory", 1000.0);

        resourceUserPool.put("derek", new HashMap<String, Number>());
        resourceUserPool.get("derek").put("cpu", 100.0);
        resourceUserPool.get("derek").put("memory", 1000.0);

        config.put(Config.RESOURCE_AWARE_SCHEDULER_USER_POOLS, resourceUserPool);
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "jerry");

        TopologyDetails topo1 = TestUtilsForResourceAwareScheduler.getTopology("topo-1", config, 1, 0, 1, 0, currentTime - 2, 20);
        TopologyDetails topo2 = TestUtilsForResourceAwareScheduler.getTopology("topo-2", config, 1, 0, 1, 0, currentTime - 2, 20);
        TopologyDetails topo3 = TestUtilsForResourceAwareScheduler.getTopology("topo-3", config, 1, 0, 1, 0, currentTime - 2, 29);
        TopologyDetails topo4 = TestUtilsForResourceAwareScheduler.getTopology("topo-4", config, 1, 0, 1, 0, currentTime - 2, 10);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "bobby");

        TopologyDetails topo5 = TestUtilsForResourceAwareScheduler.getTopology("topo-5", config, 1, 0, 1, 0, currentTime - 2, 10);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "derek");

        TopologyDetails topo6 = TestUtilsForResourceAwareScheduler.getTopology("topo-6", config, 1, 0, 1, 0, currentTime - 2, 29);

        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();
        topoMap.put(topo1.getId(), topo1);
        topoMap.put(topo2.getId(), topo2);
        topoMap.put(topo5.getId(), topo5);
        topoMap.put(topo6.getId(), topo6);

        Topologies topologies = new Topologies(topoMap);

        ResourceAwareScheduler rs = new ResourceAwareScheduler();

        rs.prepare(config);
        rs.schedule(topologies, cluster);

        for (TopologyDetails topo : rs.getUser("jerry").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("jerry").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("jerry").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("jerry").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("jerry").getTopologiesInvalid().size());


        for (TopologyDetails topo : rs.getUser("derek").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 1, rs.getUser("derek").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("derek").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("derek").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("derek").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("bobby").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 1, rs.getUser("bobby").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("bobby").getTopologiesPending().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("bobby").getTopologiesInvalid().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("bobby").getTopologiesAttempted().size());

        //user jerry submits another topology into a full cluster
        // topo3 should not be able to scheduled
        topoMap.put(topo3.getId(), topo3);
        topologies = new Topologies(topoMap);
        rs.schedule(topologies, cluster);

        for (TopologyDetails topo : rs.getUser("jerry").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("jerry").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("jerry").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 1, rs.getUser("jerry").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("jerry").getTopologiesInvalid().size());
        //make sure that topo-3 didn't get scheduled.
        Assert.assertEquals("correct topology in attempted queue", rs.getUser("jerry").getTopologiesAttempted().iterator().next().getName(), "topo-3");


        for (TopologyDetails topo : rs.getUser("derek").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 1, rs.getUser("derek").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("derek").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("derek").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("derek").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("bobby").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 1, rs.getUser("bobby").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("bobby").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("bobby").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("bobby").getTopologiesInvalid().size());

        //user jerry submits another topology but this one should be scheduled since it has higher priority than than the
        //rest of jerry's running topologies
        topoMap.put(topo4.getId(), topo4);
        topologies = new Topologies(topoMap);
        rs.schedule(topologies, cluster);

        for (TopologyDetails topo : rs.getUser("jerry").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("jerry").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("jerry").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 2, rs.getUser("jerry").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("jerry").getTopologiesInvalid().size());
        Assert.assertTrue("correct topology in attempted queue", TestUtilsForResourceAwareScheduler.findTopologyInSetFromName("topo-3", rs.getUser("jerry").getTopologiesAttempted()) != null);
        //Either topo-1 or topo-2 should have gotten evicted
        Assert.assertTrue("correct topology in attempted queue", ((TestUtilsForResourceAwareScheduler.findTopologyInSetFromName("topo-1", rs.getUser("jerry").getTopologiesAttempted())) != null)
                || (TestUtilsForResourceAwareScheduler.findTopologyInSetFromName("topo-2", rs.getUser("jerry").getTopologiesAttempted()) != null));
        //assert that topo-4 got scheduled
        Assert.assertTrue("correct topology in running queue", TestUtilsForResourceAwareScheduler.findTopologyInSetFromName("topo-4", rs.getUser("jerry").getTopologiesRunning()) != null);

        for (TopologyDetails topo : rs.getUser("derek").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 1, rs.getUser("derek").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("derek").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("derek").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("derek").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("bobby").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 1, rs.getUser("bobby").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("bobby").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("bobby").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("bobby").getTopologiesInvalid().size());
    }

    /**
     * If users are above his or her guarantee, check if topology eviction works correct
     */
    @Test
    public void TestOverGuaranteeEviction() {
        INimbus iNimbus = new TestUtilsForResourceAwareScheduler.INimbusTest();
        Map<String, Number> resourceMap = new HashMap<String, Number>();
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 100.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 1000.0);
        Map<String, SupervisorDetails> supMap = TestUtilsForResourceAwareScheduler.genSupervisors(4, 4, resourceMap);
        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY, backtype.storm.scheduler.resource.strategies.eviction.DefaultEvictionStrategy.class.getName());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, backtype.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy.class.getName());
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, backtype.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy.class.getName());
        config.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 100.0);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 500);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 500);
        Map<String, Map<String, Number>> resourceUserPool = new HashMap<String, Map<String, Number>>();
        resourceUserPool.put("jerry", new HashMap<String, Number>());
        resourceUserPool.get("jerry").put("cpu", 70.0);
        resourceUserPool.get("jerry").put("memory", 700.0);

        resourceUserPool.put("bobby", new HashMap<String, Number>());
        resourceUserPool.get("bobby").put("cpu", 100.0);
        resourceUserPool.get("bobby").put("memory", 1000.0);

        resourceUserPool.put("derek", new HashMap<String, Number>());
        resourceUserPool.get("derek").put("cpu", 25.0);
        resourceUserPool.get("derek").put("memory", 250.0);

        config.put(Config.RESOURCE_AWARE_SCHEDULER_USER_POOLS, resourceUserPool);
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "jerry");

        TopologyDetails topo1 = TestUtilsForResourceAwareScheduler.getTopology("topo-1", config, 1, 0, 1, 0, currentTime - 2, 20);
        TopologyDetails topo2 = TestUtilsForResourceAwareScheduler.getTopology("topo-2", config, 1, 0, 1, 0, currentTime - 2, 20);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "bobby");

        TopologyDetails topo3 = TestUtilsForResourceAwareScheduler.getTopology("topo-3", config, 1, 0, 1, 0, currentTime - 2, 10);
        TopologyDetails topo4 = TestUtilsForResourceAwareScheduler.getTopology("topo-4", config, 1, 0, 1, 0, currentTime - 2, 10);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "derek");

        TopologyDetails topo5 = TestUtilsForResourceAwareScheduler.getTopology("topo-5", config, 1, 0, 1, 0, currentTime - 2, 29);
        TopologyDetails topo6 = TestUtilsForResourceAwareScheduler.getTopology("topo-6", config, 1, 0, 1, 0, currentTime - 2, 10);

        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();
        topoMap.put(topo1.getId(), topo1);
        topoMap.put(topo3.getId(), topo3);
        topoMap.put(topo4.getId(), topo4);
        topoMap.put(topo5.getId(), topo5);

        Topologies topologies = new Topologies(topoMap);

        ResourceAwareScheduler rs = new ResourceAwareScheduler();

        rs.prepare(config);
        rs.schedule(topologies, cluster);

        for (TopologyDetails topo : rs.getUser("jerry").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 1, rs.getUser("jerry").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("jerry").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("jerry").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("jerry").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("derek").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 1, rs.getUser("derek").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("derek").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("derek").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("derek").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("bobby").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("bobby").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("bobby").getTopologiesPending().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("bobby").getTopologiesInvalid().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("bobby").getTopologiesAttempted().size());

        //user derek submits another topology into a full cluster
        //topo6 should not be able to scheduled intially, but since topo6 has higher priority than topo5
        //topo5 will be evicted so that topo6 can be scheduled
        topoMap.put(topo6.getId(), topo6);
        topologies = new Topologies(topoMap);
        rs.schedule(topologies, cluster);

        for (TopologyDetails topo : rs.getUser("jerry").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 1, rs.getUser("jerry").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("jerry").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("jerry").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("jerry").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("derek").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 1, rs.getUser("derek").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("derek").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 1, rs.getUser("derek").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("derek").getTopologiesInvalid().size());
        //topo5 will be evicted since topo6 has higher priority
        Assert.assertEquals("correct topology in attempted queue", "topo-5", rs.getUser("derek").getTopologiesAttempted().iterator().next().getName());

        for (TopologyDetails topo : rs.getUser("bobby").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("bobby").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("bobby").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("bobby").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("bobby").getTopologiesInvalid().size());

        //user jerry submits topo2
        topoMap.put(topo2.getId(), topo2);
        topologies = new Topologies(topoMap);
        rs.schedule(topologies, cluster);

        for (TopologyDetails topo : rs.getUser("jerry").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("jerry").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("jerry").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("jerry").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("jerry").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("derek").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 0, rs.getUser("derek").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("derek").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 2, rs.getUser("derek").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("derek").getTopologiesInvalid().size());
        Assert.assertEquals("correct topology in attempted queue", "topo-6", rs.getUser("derek").getTopologiesAttempted().iterator().next().getName());

        for (TopologyDetails topo : rs.getUser("bobby").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("bobby").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("bobby").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("bobby").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("bobby").getTopologiesInvalid().size());
    }

    /**
     * Test correct behaviour when a supervisor dies.  Check if the scheduler handles it correctly and evicts the correct
     * topology when rescheduling the executors from the died supervisor
     */
    @Test
    public void TestFaultTolerance() {
        INimbus iNimbus = new TestUtilsForResourceAwareScheduler.INimbusTest();
        Map<String, Number> resourceMap = new HashMap<String, Number>();
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 100.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 1000.0);
        Map<String, SupervisorDetails> supMap = TestUtilsForResourceAwareScheduler.genSupervisors(6, 4, resourceMap);
        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY, backtype.storm.scheduler.resource.strategies.eviction.DefaultEvictionStrategy.class.getName());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, backtype.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy.class.getName());
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, backtype.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy.class.getName());
        config.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 100.0);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 500);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 500);
        Map<String, Map<String, Number>> resourceUserPool = new HashMap<String, Map<String, Number>>();
        resourceUserPool.put("jerry", new HashMap<String, Number>());
        resourceUserPool.get("jerry").put("cpu", 50.0);
        resourceUserPool.get("jerry").put("memory", 500.0);

        resourceUserPool.put("bobby", new HashMap<String, Number>());
        resourceUserPool.get("bobby").put("cpu", 200.0);
        resourceUserPool.get("bobby").put("memory", 2000.0);

        resourceUserPool.put("derek", new HashMap<String, Number>());
        resourceUserPool.get("derek").put("cpu", 100.0);
        resourceUserPool.get("derek").put("memory", 1000.0);

        config.put(Config.RESOURCE_AWARE_SCHEDULER_USER_POOLS, resourceUserPool);
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "jerry");

        TopologyDetails topo1 = TestUtilsForResourceAwareScheduler.getTopology("topo-1", config, 1, 0, 1, 0, currentTime - 2, 20);
        TopologyDetails topo2 = TestUtilsForResourceAwareScheduler.getTopology("topo-2", config, 1, 0, 1, 0, currentTime - 2, 20);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "bobby");

        TopologyDetails topo3 = TestUtilsForResourceAwareScheduler.getTopology("topo-3", config, 1, 0, 1, 0, currentTime - 2, 10);
        TopologyDetails topo4 = TestUtilsForResourceAwareScheduler.getTopology("topo-4", config, 1, 0, 1, 0, currentTime - 2, 10);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "derek");

        TopologyDetails topo5 = TestUtilsForResourceAwareScheduler.getTopology("topo-5", config, 1, 0, 1, 0, currentTime - 2, 29);
        TopologyDetails topo6 = TestUtilsForResourceAwareScheduler.getTopology("topo-6", config, 1, 0, 1, 0, currentTime - 2, 10);

        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();
        topoMap.put(topo1.getId(), topo1);
        topoMap.put(topo2.getId(), topo2);
        topoMap.put(topo3.getId(), topo3);
        topoMap.put(topo4.getId(), topo4);
        topoMap.put(topo5.getId(), topo5);
        topoMap.put(topo6.getId(), topo6);

        Topologies topologies = new Topologies(topoMap);

        ResourceAwareScheduler rs = new ResourceAwareScheduler();

        rs.prepare(config);
        rs.schedule(topologies, cluster);

        for (TopologyDetails topo : rs.getUser("jerry").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("jerry").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("jerry").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("jerry").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("jerry").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("derek").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("derek").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("derek").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("derek").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("derek").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("bobby").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("bobby").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("bobby").getTopologiesPending().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("bobby").getTopologiesInvalid().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("bobby").getTopologiesAttempted().size());

        //fail supervisor
        SupervisorDetails supFailed = cluster.getSupervisors().values().iterator().next();
        LOG.info("/***** failing supervisor: {} ****/", supFailed.getHost());
        supMap.remove(supFailed.getId());
        Map<String, SchedulerAssignmentImpl> newAssignments = new HashMap<String, SchedulerAssignmentImpl>();
        for (Map.Entry<String, SchedulerAssignment> topoToAssignment : cluster.getAssignments().entrySet()) {
            String topoId = topoToAssignment.getKey();
            SchedulerAssignment assignment = topoToAssignment.getValue();
            Map<ExecutorDetails, WorkerSlot> executorToSlots = new HashMap<ExecutorDetails, WorkerSlot>();
            for (Map.Entry<ExecutorDetails, WorkerSlot> execToWorker : assignment.getExecutorToSlot().entrySet()) {
                ExecutorDetails exec = execToWorker.getKey();
                WorkerSlot ws = execToWorker.getValue();
                if (!ws.getNodeId().equals(supFailed.getId())) {
                    executorToSlots.put(exec, ws);
                }
            }
            newAssignments.put(topoId, new SchedulerAssignmentImpl(topoId, executorToSlots));
        }
        Map<String, String> statusMap = cluster.getStatusMap();
        cluster = new Cluster(iNimbus, supMap, newAssignments, config);
        cluster.setStatusMap(statusMap);

        rs.schedule(topologies, cluster);

        //Supervisor failed contains a executor from topo-6 of user derek.  Should evict a topology from user jerry since user will be above resource guarantee more so than user derek
        for (TopologyDetails topo : rs.getUser("jerry").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 1, rs.getUser("jerry").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("jerry").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 1, rs.getUser("jerry").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("jerry").getTopologiesInvalid().size());


        for (TopologyDetails topo : rs.getUser("derek").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("derek").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("derek").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("derek").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("derek").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("bobby").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("bobby").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("bobby").getTopologiesPending().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("bobby").getTopologiesInvalid().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("bobby").getTopologiesAttempted().size());
    }
}
