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
import backtype.storm.scheduler.resource.strategies.eviction.IEvictionStrategy;
import backtype.storm.scheduler.resource.strategies.priority.ISchedulingPriorityStrategy;
import backtype.storm.scheduler.resource.strategies.scheduling.IStrategy;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ResourceAwareScheduler implements IScheduler {

    private Map<String, User> userMap;
    private Cluster cluster;
    private Topologies topologies;
    private RAS_Nodes nodes;

    private class SchedulingState {
        private Map<String, User> userMap = new HashMap<String, User>();
        private Cluster cluster;
        private Topologies topologies;
        private RAS_Nodes nodes;
        private Map conf = new Config();

        public SchedulingState(Map<String, User> userMap, Cluster cluster, Topologies topologies, RAS_Nodes nodes, Map conf) {
            for(Map.Entry<String, User> userMapEntry : userMap.entrySet()) {
                String userId = userMapEntry.getKey();
                User user = userMapEntry.getValue();
                this.userMap.put(userId, user.getCopy());
            }
            this.cluster = cluster.getCopy();
            this.topologies = topologies.getCopy();
            this.nodes = new RAS_Nodes(this.cluster, this.topologies);
            this.conf.putAll(conf);

        }
    }


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
        LOG.info("topologies: {}", topologies);

        this.initialize(topologies, cluster);

        LOG.info("UserMap:\n{}", this.userMap);
        for (User user : this.getUserMap().values()) {
            LOG.info(user.getDetailedInfo());
        }

        for (TopologyDetails topo : topologies.getTopologies()) {
            LOG.info("topo {} status: {}", topo, cluster.getStatusMap().get(topo.getId()));
        }

        LOG.info("Nodes:\n{}", this.nodes);

        //LOG.info("getNextUser: {}", this.getNextUser());

        ISchedulingPriorityStrategy schedulingPrioritystrategy = null;
        while (true) {
            LOG.info("/*********** next scheduling iteration **************/");

            if(schedulingPrioritystrategy == null) {
                try {
                    schedulingPrioritystrategy = (ISchedulingPriorityStrategy) Utils.newInstance((String) this.conf.get(Config.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY));
                } catch (RuntimeException e) {
                    LOG.error("failed to create instance of priority strategy: {} with error: {}! No topology eviction will be done.",
                            this.conf.get(Config.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY), e.getMessage());
                    break;
                }
            }
            //need to re prepare since scheduling state might have been restored
            schedulingPrioritystrategy.prepare(this.topologies, this.cluster, this.userMap, this.nodes);
            //Call scheduling priority strategy
            TopologyDetails td = schedulingPrioritystrategy.getNextTopologyToSchedule();
            if(td == null) {
                break;
            }
            scheduleTopology(td);
        }

        //since scheduling state might have been restored thus need to set the cluster and topologies.
        cluster = this.cluster;
        topologies = this.topologies;
    }

    public void scheduleTopology(TopologyDetails td) {
        User topologySubmitter = this.userMap.get(td.getTopologySubmitter());
        if (cluster.getUnassignedExecutors(td).size() > 0) {
            LOG.info("/********Scheduling topology {} from User {}************/", td.getName(), topologySubmitter);
            LOG.info("{}", this.userMap.get(td.getTopologySubmitter()).getDetailedInfo());
            LOG.info("{}", User.getResourcePoolAverageUtilizationForUsers(this.userMap.values()));
            LOG.info("Nodes:\n{}", this.nodes);
            LOG.debug("From cluster:\n{}", ResourceUtils.printScheduling(this.cluster, this.topologies));
            LOG.debug("From Nodes:\n{}", ResourceUtils.printScheduling(this.nodes));

            SchedulingState schedulingState = this.checkpointSchedulingState();
            IStrategy RAStrategy = null;
            try {
                RAStrategy = (IStrategy) Utils.newInstance((String) td.getConf().get(Config.TOPOLOGY_SCHEDULER_STRATEGY));
            } catch (RuntimeException e) {
                LOG.error("failed to create instance of IStrategy: {} with error: {}! Topology {} will not be scheduled.",
                        td.getName(), td.getConf().get(Config.TOPOLOGY_SCHEDULER_STRATEGY), e.getMessage());
                topologySubmitter.moveTopoFromPendingToInvalid(td, this.cluster);
                return;
            }
            IEvictionStrategy evictionStrategy = null;
            while (true) {
                //Need to re prepare scheduling strategy with cluster and topologies in case scheduling state was restored
                RAStrategy.prepare(this.topologies, this.cluster, this.userMap, this.nodes);
                SchedulingResult result = RAStrategy.schedule(td);
                LOG.info("scheduling result: {}", result);
                if (result.isValid()) {
                    if (result.isSuccess()) {
                        try {
                            if (mkAssignment(td, result.getSchedulingResultMap())) {
                                topologySubmitter.moveTopoFromPendingToRunning(td, this.cluster);
                            } else {
                                this.restoreCheckpointSchedulingState(schedulingState);
                                //since state is restored need the update User topologySubmitter to the new User object in userMap
                                topologySubmitter = this.userMap.get(td.getTopologySubmitter());
                                topologySubmitter.moveTopoFromPendingToAttempted(td, this.cluster);
                            }
                        } catch (IllegalStateException ex) {
                            LOG.error(ex.toString());
                            LOG.error("Unsuccessful in scheduling: IllegalStateException thrown!", td.getId());
                            this.cluster.setStatus(td.getId(), "Unsuccessful in scheduling");
                            this.restoreCheckpointSchedulingState(schedulingState);
                            //since state is restored need the update User topologySubmitter to the new User object in userMap
                            topologySubmitter = this.userMap.get(td.getTopologySubmitter());
                            topologySubmitter.moveTopoFromPendingToAttempted(td, this.cluster);
                        }
                        break;
                    } else {
                        if (result.getStatus() == SchedulingStatus.FAIL_NOT_ENOUGH_RESOURCES) {
                            if(evictionStrategy == null) {
                                try {
                                    evictionStrategy = (IEvictionStrategy) Utils.newInstance((String) this.conf.get(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY));
                                } catch (RuntimeException e) {
                                    LOG.error("failed to create instance of eviction strategy: {} with error: {}! No topology eviction will be done.",
                                            this.conf.get(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY), e.getMessage());
                                    topologySubmitter.moveTopoFromPendingToAttempted(td);
                                    break;
                                }
                            }
                            //need to re prepare since scheduling state might have been restored
                            evictionStrategy.prepare(this.topologies, this.cluster, this.userMap, this.nodes);
                            if (!evictionStrategy.makeSpaceForTopo(td)) {
                                this.cluster.setStatus(td.getId(), result.getErrorMessage());
                                this.restoreCheckpointSchedulingState(schedulingState);
                                //since state is restored need the update User topologySubmitter to the new User object in userMap
                                topologySubmitter = this.userMap.get(td.getTopologySubmitter());
                                topologySubmitter.moveTopoFromPendingToAttempted(td);
                                break;
                            }
                            continue;
                        } else if (result.getStatus() == SchedulingStatus.FAIL_INVALID_TOPOLOGY) {
                            this.restoreCheckpointSchedulingState(schedulingState);
                            //since state is restored need the update User topologySubmitter to the new User object in userMap
                            topologySubmitter = this.userMap.get(td.getTopologySubmitter());
                            topologySubmitter.moveTopoFromPendingToInvalid(td, this.cluster);
                            break;
                        } else {
                            this.restoreCheckpointSchedulingState(schedulingState);
                            //since state is restored need the update User topologySubmitter to the new User object in userMap
                            topologySubmitter = this.userMap.get(td.getTopologySubmitter());
                            topologySubmitter.moveTopoFromPendingToAttempted(td, this.cluster);
                            break;
                        }
                    }
                } else {
                    LOG.warn("Scheduling results returned from topology {} is not vaild! Topology with be ignored.", td.getName());
                    this.restoreCheckpointSchedulingState(schedulingState);
                    //since state is restored need the update User topologySubmitter to the new User object in userMap
                    topologySubmitter = this.userMap.get(td.getTopologySubmitter());
                    topologySubmitter.moveTopoFromPendingToInvalid(td, this.cluster);
                    break;
                }
            }
        } else {
            LOG.warn("Topology {} is already fully scheduled!", td.getName());
            topologySubmitter.moveTopoFromPendingToRunning(td, this.cluster);
            throw new IllegalStateException("illegal");
        }
    }

    private boolean mkAssignment(TopologyDetails td, Map<WorkerSlot, Collection<ExecutorDetails>> schedulerAssignmentMap) {
        LOG.info("making assignments for topology {}", td);
        if (schedulerAssignmentMap != null) {
            double requestedMemOnHeap = td.getTotalRequestedMemOnHeap();
            double requestedMemOffHeap = td.getTotalRequestedMemOffHeap();
            double requestedCpu = td.getTotalRequestedCpu();
            double assignedMemOnHeap = 0.0;
            double assignedMemOffHeap = 0.0;
            double assignedCpu = 0.0;

            Set<String> nodesUsed = new HashSet<String>();
            int assignedWorkers = schedulerAssignmentMap.keySet().size();
            for (Map.Entry<WorkerSlot, Collection<ExecutorDetails>> workerToTasksEntry : schedulerAssignmentMap.entrySet()) {
                WorkerSlot targetSlot = workerToTasksEntry.getKey();
                Collection<ExecutorDetails> execsNeedScheduling = workerToTasksEntry.getValue();
                RAS_Node targetNode = this.nodes.getNodeById(targetSlot.getNodeId());
                targetNode.assign(targetSlot, td, execsNeedScheduling);
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

            Double[] resources = {requestedMemOnHeap, requestedMemOffHeap, requestedCpu,
                    assignedMemOnHeap, assignedMemOffHeap, assignedCpu};
            LOG.debug("setResources for {}: requested on-heap mem, off-heap mem, cpu: {} {} {} " +
                            "assigned on-heap mem, off-heap mem, cpu: {} {} {}",
                    td.getId(), requestedMemOnHeap, requestedMemOffHeap, requestedCpu,
                    assignedMemOnHeap, assignedMemOffHeap, assignedCpu);
            this.cluster.setResources(td.getId(), resources);
            return true;
        } else {
            LOG.warn("schedulerAssignmentMap for topo {} is null. This shouldn't happen!", td.getName());
            return false;
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

//    public User getNextUser() {
//        Double least = Double.POSITIVE_INFINITY;
//        User ret = null;
//        for (User user : this.userMap.values()) {
//            LOG.info("getNextUser {}", user.getDetailedInfo());
//            LOG.info("hasTopologyNeedSchedule: {}", user.hasTopologyNeedSchedule());
//            if (user.hasTopologyNeedSchedule()) {
//                Double userResourcePoolAverageUtilization = user.getResourcePoolAverageUtilization();
//                if(ret!=null) {
//                    LOG.info("current: {}-{} compareUser: {}-{}", ret.getId(), least, user.getId(), userResourcePoolAverageUtilization);
//                    LOG.info("{} == {}: {}", least, userResourcePoolAverageUtilization, least == userResourcePoolAverageUtilization);
//                    LOG.info("{} == {}: {}", least, userResourcePoolAverageUtilization, (Math.abs(least - userResourcePoolAverageUtilization) < 0.0001));
//
//                }
//                if (least > userResourcePoolAverageUtilization) {
//                    ret = user;
//                    least = userResourcePoolAverageUtilization;
//                } else if (Math.abs(least - userResourcePoolAverageUtilization) < 0.0001) {
//                    double currentCpuPercentage = ret.getCPUResourceGuaranteed() / this.cluster.getClusterTotalCPUResource();
//                    double currentMemoryPercentage = ret.getMemoryResourceGuaranteed() / this.cluster.getClusterTotalMemoryResource();
//                    double currentAvgPercentage = (currentCpuPercentage + currentMemoryPercentage) / 2.0;
//
//                    double userCpuPercentage = user.getCPUResourceGuaranteed() / this.cluster.getClusterTotalCPUResource();
//                    double userMemoryPercentage = user.getMemoryResourceGuaranteed() / this.cluster.getClusterTotalMemoryResource();
//                    double userAvgPercentage = (userCpuPercentage + userMemoryPercentage) / 2.0;
//                    LOG.info("current: {}-{} compareUser: {}-{}", ret.getId(), currentAvgPercentage, user.getId(), userAvgPercentage);
//                    if (userAvgPercentage > currentAvgPercentage) {
//                        ret = user;
//                        least = userResourcePoolAverageUtilization;
//                    }
//                }
//            }
//        }
//        return ret;
//    }

    /**
     * Intialize scheduling and running queues
     *
     * @param topologies
     * @param cluster
     */
    private void initUsers(Topologies topologies, Cluster cluster) {

        this.userMap = new HashMap<String, User>();
        Map<String, Map<String, Double>> userResourcePools = this.getUserResourcePools();
        LOG.info("userResourcePools: {}", userResourcePools);

        for (TopologyDetails td : topologies.getTopologies()) {
            String topologySubmitter = td.getTopologySubmitter();
            if (topologySubmitter == null) {
                LOG.warn("Topology {} submitted by anonymous user", td.getName());
                topologySubmitter = "anonymous";
            }
            if (!this.userMap.containsKey(topologySubmitter)) {
                this.userMap.put(topologySubmitter, new User(topologySubmitter, userResourcePools.get(topologySubmitter)));
            }
            if (cluster.getUnassignedExecutors(td).size() >= td.getExecutors().size()) {
                this.userMap.get(topologySubmitter).addTopologyToPendingQueue(td, cluster);
            } else {
                this.userMap.get(topologySubmitter).addTopologyToRunningQueue(td, cluster);
            }
        }
    }

    private void initialize(Topologies topologies, Cluster cluster) {
        initUsers(topologies, cluster);
        this.cluster = cluster;
        this.topologies = topologies;
        this.nodes = new RAS_Nodes(this.cluster, this.topologies);
    }

    /**
     * Get resource guarantee configs
     *
     * @return
     */
    private Map<String, Map<String, Double>> getUserResourcePools() {
        Object raw = this.conf.get(Config.RESOURCE_AWARE_SCHEDULER_USER_POOLS);
        Map<String, Map<String, Double>> ret = new HashMap<String, Map<String, Double>>();

        if (raw != null) {
            for (Map.Entry<String, Map<String, Number>> UserPoolEntry : ((Map<String, Map<String, Number>>) raw).entrySet()) {
                String user = UserPoolEntry.getKey();
                ret.put(user, new HashMap<String, Double>());
                for (Map.Entry<String, Number> resourceEntry : UserPoolEntry.getValue().entrySet()) {
                    ret.get(user).put(resourceEntry.getKey(), resourceEntry.getValue().doubleValue());
                }
            }
        }

        Map fromFile = Utils.findAndReadConfigFile("user-resource-pools.yaml", false);
        Map<String, Map<String, Number>> tmp = (Map<String, Map<String, Number>>) fromFile.get(Config.RESOURCE_AWARE_SCHEDULER_USER_POOLS);
        if (tmp != null) {
            for (Map.Entry<String, Map<String, Number>> UserPoolEntry : tmp.entrySet()) {
                String user = UserPoolEntry.getKey();
                ret.put(user, new HashMap<String, Double>());
                for (Map.Entry<String, Number> resourceEntry : UserPoolEntry.getValue().entrySet()) {
                    ret.get(user).put(resourceEntry.getKey(), resourceEntry.getValue().doubleValue());
                }
            }
        }
        return ret;
    }

    private SchedulingState checkpointSchedulingState() {
        LOG.info("checkpointing scheduling state...");
        LOG.info("/*********Checkpoint************/");
        for (User user : this.getUserMap().values()) {
            LOG.info(user.getDetailedInfo());
        }
        LOG.info("/*********End************/");
        return new SchedulingState(this.userMap, this.cluster, this.topologies, this.nodes, this.conf);
    }

    private void restoreCheckpointSchedulingState(SchedulingState schedulingState) {
        LOG.info("restoring scheduling state...");
        LOG.info("/*********Before************/");
        for (User user : this.getUserMap().values()) {
            LOG.info(user.getDetailedInfo());
        }
        this.cluster = schedulingState.cluster;
        this.topologies = schedulingState.topologies;
        this.conf = schedulingState.conf;
        this.userMap = schedulingState.userMap;
        this.nodes = schedulingState.nodes;
        LOG.info("/*********After************/");
        for (User user : this.getUserMap().values()) {
            LOG.info(user.getDetailedInfo());
        }
        LOG.info("/*********End************/");
    }
}
