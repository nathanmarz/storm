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
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.scheduler.resource.strategies.ResourceAwareStrategy;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ResourceAwareScheduler implements IScheduler {

    private Map<String, User> userMap = new HashMap<String, User>();
    private Cluster cluster;
    private Topologies topologies;


    @SuppressWarnings("rawtypes")
    private Map conf;

    private static final Logger LOG = LoggerFactory
            .getLogger(ResourceAwareScheduler.class);

    @Override
    public void prepare(Map conf) {
        this.conf = conf;
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        LOG.info("\n\n\nRerunning ResourceAwareScheduler...");
        LOG.debug(ResourceUtils.printScheduling(cluster, topologies));

        this.initialize(topologies, cluster);


        LOG.info("UserMap:\n{}", this.userMap);
        for(User user : this.getUserMap().values()) {
            LOG.info(user.getDetailedInfo());
        }

        LOG.info("getNextUser: {}", this.getNextUser());

        while(true) {
            User nextUser = this.getNextUser();
            if(nextUser == null){
                break;
            }
            TopologyDetails td = nextUser.getNextTopologyToSchedule();
            scheduleTopology(td);
        }
    }

    private void scheduleTopology(TopologyDetails td) {
        ResourceAwareStrategy RAStrategy = new ResourceAwareStrategy(this.cluster, this.topologies);
        if (cluster.needsScheduling(td) && cluster.getUnassignedExecutors(td).size() > 0) {
            LOG.info("/********Scheduling topology {} from User {}************/", td.getName(), td.getTopologySubmitter());
            LOG.info("{}", this.userMap.get(td.getTopologySubmitter()).getDetailedInfo());
            LOG.info("{}", User.getResourcePoolAverageUtilizationForUsers(this.userMap.values()));

            Map<WorkerSlot, Collection<ExecutorDetails>> schedulerAssignmentMap = RAStrategy.schedule(td);

            double requestedMemOnHeap = td.getTotalRequestedMemOnHeap();
            double requestedMemOffHeap = td.getTotalRequestedMemOffHeap();
            double requestedCpu = td.getTotalRequestedCpu();
            double assignedMemOnHeap = 0.0;
            double assignedMemOffHeap = 0.0;
            double assignedCpu = 0.0;

            if (schedulerAssignmentMap != null) {
                try {
                    Set<String> nodesUsed = new HashSet<String>();
                    int assignedWorkers = schedulerAssignmentMap.keySet().size();
                    for (Map.Entry<WorkerSlot, Collection<ExecutorDetails>> workerToTasksEntry : schedulerAssignmentMap.entrySet()) {
                        WorkerSlot targetSlot = workerToTasksEntry.getKey();
                        Collection<ExecutorDetails> execsNeedScheduling = workerToTasksEntry.getValue();
                        RAS_Node targetNode = RAStrategy.idToNode(targetSlot.getNodeId());
                        targetNode.assign(targetSlot, td, execsNeedScheduling, this.cluster);
                        LOG.debug("ASSIGNMENT    TOPOLOGY: {}  TASKS: {} To Node: {} on Slot: {}",
                                td.getName(), execsNeedScheduling, targetNode.getHostname(), targetSlot.getPort());
                        if (!nodesUsed.contains(targetNode.getId())) {
                            nodesUsed.add(targetNode.getId());
                        }
                        assignedMemOnHeap += targetSlot.getAllocatedMemOnHeap();
                        assignedMemOffHeap += targetSlot.getAllocatedMemOffHeap();
                        assignedCpu += targetSlot.getAllocatedCpu();
                    }
                    LOG.debug("Topology: {} assigned to {} nodes on {} workers", td.getId(), nodesUsed.size(), assignedWorkers);
                    this.cluster.setStatus(td.getId(), "Fully Scheduled");
                    this.getUser(td.getTopologySubmitter()).moveTopoFromPendingToRunning(td);
                    LOG.info("getNextUser: {}", this.getNextUser());
                } catch (IllegalStateException ex) {
                    LOG.error(ex.toString());
                    LOG.error("Unsuccessful in scheduling", td.getId());
                    this.cluster.setStatus(td.getId(), "Unsuccessful in scheduling");
                    this.getUser(td.getTopologySubmitter()).moveTopoFromPendingToAttempted(td);
                }
            } else {
                LOG.error("Unsuccessful in scheduling {}", td.getId());
                this.cluster.setStatus(td.getId(), "Unsuccessful in scheduling");
                this.getUser(td.getTopologySubmitter()).moveTopoFromPendingToAttempted(td);
            }
            Double[] resources = {requestedMemOnHeap, requestedMemOffHeap, requestedCpu,
                    assignedMemOnHeap, assignedMemOffHeap, assignedCpu};
            LOG.debug("setResources for {}: requested on-heap mem, off-heap mem, cpu: {} {} {} " +
                            "assigned on-heap mem, off-heap mem, cpu: {} {} {}",
                    td.getId(), requestedMemOnHeap, requestedMemOffHeap, requestedCpu,
                    assignedMemOnHeap, assignedMemOffHeap, assignedCpu);
            this.cluster.setResources(td.getId(), resources);
        } else {
            LOG.warn("Topology {} already scheduled!", td.getName());
            this.cluster.setStatus(td.getId(), "Fully Scheduled");
        }
        updateSupervisorsResources(cluster, topologies);
    }

    private void updateSupervisorsResources(Cluster cluster, Topologies topologies) {
        Map<String, Double[]> supervisors_resources = new HashMap<String, Double[]>();
        Map<String, RAS_Node> nodes = RAS_Node.getAllNodesFrom(cluster, topologies);
        for (Map.Entry<String, RAS_Node> entry : nodes.entrySet()) {
            RAS_Node node = entry.getValue();
            Double totalMem = node.getTotalMemoryResources();
            Double totalCpu = node.getTotalCpuResources();
            Double usedMem = totalMem - node.getAvailableMemoryResources();
            Double usedCpu = totalCpu - node.getAvailableCpuResources();
            Double[] resources = {totalMem, totalCpu, usedMem, usedCpu};
            supervisors_resources.put(entry.getKey(), resources);
        }
        cluster.setSupervisorsResourcesMap(supervisors_resources);
    }
    public User getUser(String user) {
        return this.userMap.get(user);
    }

    public Map<String, User> getUserMap() {
        return this.userMap;
    }

    public User getNextUser() {
        Double least = Double.MAX_VALUE;
        User ret = null;
        for(User user : this.userMap.values()) {
            if(user.hasTopologyNeedSchedule()) {
                Double userResourcePoolAverageUtilization = user.getResourcePoolAverageUtilization();
                if (least > userResourcePoolAverageUtilization) {
                    ret = user;
                    least = userResourcePoolAverageUtilization;
                } else if (least == userResourcePoolAverageUtilization) {
                    double currentCpuPercentage = ret.getCPUResourceGuaranteed()/this.cluster.getClusterTotalCPUResource();
                    double currentMemoryPercentage = ret.getMemoryResourceGuaranteed()/this.cluster.getClusterTotalMemoryResource();
                    double currentAvgPercentage = (currentCpuPercentage + currentMemoryPercentage) / 2.0;

                    double userCpuPercentage = user.getCPUResourceGuaranteed()/this.cluster.getClusterTotalCPUResource();
                    double userMemoryPercentage = user.getMemoryResourceGuaranteed()/this.cluster.getClusterTotalMemoryResource();
                    double userAvgPercentage = (userCpuPercentage + userMemoryPercentage) / 2.0;
                    if(userAvgPercentage > currentAvgPercentage) {
                        ret = user;
                        least = userResourcePoolAverageUtilization;
                    }
                }
            }
        }
        return ret;
    }

    /**
     * Intialize scheduling and running queues
     * @param topologies
     * @param cluster
     */
    private void initUsers(Topologies topologies, Cluster cluster) {

        Map<String, Map<String, Double>> userResourcePools = this.getUserResourcePools();
        LOG.info("userResourcePools: {}", userResourcePools);

        for (TopologyDetails topo : topologies.getTopologies()) {
            String topologySubmitter = topo.getTopologySubmitter();
            if(!this.userMap.containsKey(topologySubmitter)) {
                this.userMap.put(topologySubmitter, new User(topologySubmitter, userResourcePools.get(topologySubmitter)));
            }
            if(cluster.getUnassignedExecutors(topo).size() >= topo.getExecutors().size()) {
                this.userMap.get(topologySubmitter).addTopologyToPendingQueue(topo);
            } else {
                this.userMap.get(topologySubmitter).addTopologyToRunningQueue(topo);
            }
        }
    }

    private void initialize(Topologies topologies, Cluster cluster) {
        initUsers(topologies, cluster);
        this.cluster = cluster;
        this.topologies = topologies;
    }

    /**
     * Get resource guarantee configs
     * @return
     */
    private Map<String, Map<String, Double>> getUserResourcePools() {

        Map<String, Map<String, Number>> raw =  (Map<String, Map<String, Number>>)this.conf.get(Config.RESOURCE_AWARE_SCHEDULER_USER_POOLS);
        Map<String, Map<String, Double>> ret =  (Map<String, Map<String, Double>>)this.conf.get(Config.RESOURCE_AWARE_SCHEDULER_USER_POOLS);

        if (raw == null) {
            ret = new  HashMap<String, Map<String, Double>>();
        } else {
            for(Map.Entry<String, Map<String, Number>> UserPoolEntry : raw.entrySet()) {
                String user = UserPoolEntry.getKey();
                ret.put(user, new HashMap<String, Double>());
                for(Map.Entry<String, Number> resourceEntry : UserPoolEntry.getValue().entrySet()) {
                    ret.get(user).put(resourceEntry.getKey(), resourceEntry.getValue().doubleValue());
                }
            }
        }

        Map fromFile = Utils.findAndReadConfigFile("user-resource-pools.yaml", false);
        Map<String, Map<String, Number>>tmp = (Map<String, Map<String, Number>>)fromFile.get(Config.RESOURCE_AWARE_SCHEDULER_USER_POOLS);
        if (tmp != null) {
            for(Map.Entry<String, Map<String, Number>> UserPoolEntry : tmp.entrySet()) {
                String user = UserPoolEntry.getKey();
                ret.put(user, new HashMap<String, Double>());
                for(Map.Entry<String, Number> resourceEntry : UserPoolEntry.getValue().entrySet()) {
                    ret.get(user).put(resourceEntry.getKey(), resourceEntry.getValue().doubleValue());
                }
            }
        }
        return ret;
    }
}
