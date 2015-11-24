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

package backtype.storm.scheduler.resource.strategies.eviction;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.scheduler.resource.RAS_Nodes;
import backtype.storm.scheduler.resource.ResourceUtils;
import backtype.storm.scheduler.resource.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class DefaultEvictionStrategy implements IEvictionStrategy {
    private static final Logger LOG = LoggerFactory
            .getLogger(DefaultEvictionStrategy.class);

    private Topologies topologies;
    private Cluster cluster;
    private Map<String, User> userMap;
    private RAS_Nodes nodes;

    @Override
    public void prepare(Topologies topologies, Cluster cluster, Map<String, User> userMap, RAS_Nodes nodes) {
        this.topologies = topologies;
        this.cluster = cluster;
        this.userMap = userMap;
        this.nodes = nodes;
    }

    @Override
    public boolean makeSpaceForTopo(TopologyDetails td) {
        LOG.info("attempting to make space for topo {} from user {}", td.getName(), td.getTopologySubmitter());
        User submitter = this.userMap.get(td.getTopologySubmitter());
        if (submitter.getCPUResourceGuaranteed() == null || submitter.getMemoryResourceGuaranteed() == null) {
            return false;
        }

        double cpuNeeded = td.getTotalRequestedCpu() / submitter.getCPUResourceGuaranteed();
        double memoryNeeded = (td.getTotalRequestedMemOffHeap() + td.getTotalRequestedMemOnHeap()) / submitter.getMemoryResourceGuaranteed();

        //user has enough resource under his or her resource guarantee to schedule topology
        if ((1.0 - submitter.getCPUResourcePoolUtilization()) >= cpuNeeded && (1.0 - submitter.getMemoryResourcePoolUtilization()) >= memoryNeeded) {
            User evictUser = this.findUserWithMostResourcesAboveGuarantee();
            if (evictUser == null) {
                LOG.info("Cannot make space for topology {} from user {}", td.getName(), submitter.getId());
                submitter.moveTopoFromPendingToAttempted(td, this.cluster);

                return false;
            }
            TopologyDetails topologyEvict = evictUser.getRunningTopologyWithLowestPriority();
            LOG.info("topology to evict: {}", topologyEvict);
            evictTopology(topologyEvict);

            LOG.info("Resources After eviction:\n{}", this.nodes);

            return true;
        } else {

            if ((1.0 - submitter.getCPUResourcePoolUtilization()) < cpuNeeded) {

            }

            if ((1.0 - submitter.getMemoryResourcePoolUtilization()) < memoryNeeded) {

            }
            return false;

        }
    }

    private void evictTopology(TopologyDetails topologyEvict) {
        Collection<WorkerSlot> workersToEvict = this.cluster.getUsedSlotsByTopologyId(topologyEvict.getId());
        User submitter = this.userMap.get(topologyEvict.getTopologySubmitter());

        LOG.info("Evicting Topology {} with workers: {}", topologyEvict.getName(), workersToEvict);
        LOG.debug("From Nodes:\n{}", ResourceUtils.printScheduling(this.nodes));
        this.nodes.freeSlots(workersToEvict);
        submitter.moveTopoFromRunningToPending(topologyEvict, this.cluster);
        LOG.info("check if topology unassigned: {}", this.cluster.getUsedSlotsByTopologyId(topologyEvict.getId()));
    }

    private User findUserWithMostResourcesAboveGuarantee() {
        double most = 0.0;
        User mostOverUser = null;
        for (User user : this.userMap.values()) {
            double over = user.getResourcePoolAverageUtilization() - 1.0;
            if ((over > most) && (!user.getTopologiesRunning().isEmpty())) {
                most = over;
                mostOverUser = user;
            }
        }
        return mostOverUser;
    }
}
