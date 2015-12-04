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

import backtype.storm.scheduler.TopologyDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class User {
    private String userId;
    //Topologies yet to be scheduled sorted by priority for each user
    private Set<TopologyDetails> pendingQueue = new TreeSet<TopologyDetails>(new PQsortByPriorityAndSubmittionTime());

    //Topologies yet to be scheduled sorted by priority for each user
    private Set<TopologyDetails> runningQueue = new TreeSet<TopologyDetails>(new PQsortByPriorityAndSubmittionTime());

    //Topologies that was attempted to be scheduled but wasn't successull
    private Set<TopologyDetails> attemptedQueue = new TreeSet<TopologyDetails>(new PQsortByPriorityAndSubmittionTime());

    private Map<String, Double> resourcePool = new HashMap<String, Double>();

    private static final Logger LOG = LoggerFactory.getLogger(User.class);

    public User(String userId) {
        this.userId = userId;
    }

    public User(String userId, Map<String, Double> resourcePool) {
        this(userId);
        if (resourcePool != null) {
            this.resourcePool.putAll(resourcePool);
        }
    }

    public String getId() {
        return this.userId;
    }

    public void addTopologyToPendingQueue(TopologyDetails topo) {
        this.pendingQueue.add(topo);
    }

    public void addTopologyToRunningQueue(TopologyDetails topo) {
        this.runningQueue.add(topo);
    }

    public Set<TopologyDetails> getTopologiesPending() {
        TreeSet<TopologyDetails> ret = new TreeSet<TopologyDetails>(new PQsortByPriorityAndSubmittionTime());
        ret.addAll(this.pendingQueue);
        return ret;
    }

    public Set<TopologyDetails> getTopologiesRunning() {
        TreeSet<TopologyDetails> ret = new TreeSet<TopologyDetails>(new PQsortByPriorityAndSubmittionTime());
        ret.addAll(this.runningQueue);
        return ret;
    }

    public Set<TopologyDetails> getTopologiesAttempted() {
        TreeSet<TopologyDetails> ret = new TreeSet<TopologyDetails>(new PQsortByPriorityAndSubmittionTime());
        ret.addAll(this.attemptedQueue);
        return ret;
    }

    public Map<String, Number> getResourcePool() {
        if (this.resourcePool != null) {
            return new HashMap<String, Number>(this.resourcePool);
        }
        return null;
    }

    public void moveTopoFromPendingToRunning(TopologyDetails topo) {
        moveTopology(topo, this.pendingQueue, "pending", this.runningQueue, "running");
    }

    public void moveTopoFromPendingToAttempted(TopologyDetails topo) {
        moveTopology(topo, this.pendingQueue, "pending", this.attemptedQueue, "attempted");
    }

    private void moveTopology(TopologyDetails topo, Set<TopologyDetails> src, String srcName, Set<TopologyDetails> dest, String destName)  {
        if(topo == null) {
            return;
        }
        if(!src.contains(topo)) {
            LOG.warn("Topo {} not in User: {} {} queue!", topo.getName(), this.userId, srcName);
            return;
        }
        if(dest.contains(topo)) {
            LOG.warn("Topo {} already in in User: {} {} queue!", topo.getName(), this.userId, destName);
            return;
        }
        src.remove(topo);
        dest.add(topo);
    }


    public Double getResourcePoolAverageUtilization() {
        List<Double> resourceUilitzationList = new LinkedList<Double>();
        Double cpuResourcePoolUtilization = this.getCPUResourcePoolUtilization();
        Double memoryResourcePoolUtilization = this.getMemoryResourcePoolUtilization();

        if(cpuResourcePoolUtilization != null && memoryResourcePoolUtilization != null) {
            return (cpuResourcePoolUtilization + memoryResourcePoolUtilization ) / 2.0;
        }
        return Double.MAX_VALUE;
    }

    public Double getCPUResourcePoolUtilization() {
        Double cpuGuarantee = this.resourcePool.get("cpu");
        if (cpuGuarantee != null) {
            return this.getCPUResourceUsedByUser() / cpuGuarantee;
        }
        return null;
    }

    public Double getMemoryResourcePoolUtilization() {
        Double memoryGuarantee = this.resourcePool.get("memory");
        if (memoryGuarantee != null) {
            return this.getMemoryResourceUsedByUser() / memoryGuarantee;
        }
        return null;
    }


    public Double getCPUResourceUsedByUser() {
        Double sum = 0.0;
        for (TopologyDetails topo : this.runningQueue) {
            sum += topo.getTotalRequestedCpu();
        }
        return sum;
    }

    public Double getMemoryResourceUsedByUser() {
        Double sum = 0.0;
        for (TopologyDetails topo : this.runningQueue) {
            sum += topo.getTotalRequestedMemOnHeap() + topo.getTotalRequestedMemOffHeap();
        }
        return sum;
    }

    public Double getMemoryResourceGuaranteed() {
        return this.resourcePool.get("memory");
    }

    public Double getCPUResourceGuaranteed() {
        return this.resourcePool.get("cpu");
    }

    public TopologyDetails getNextTopologyToSchedule() {
        for (TopologyDetails topo : this.pendingQueue) {
            if(!this.attemptedQueue.contains(topo)) {
                return topo;
            }
        }
        return null;
    }

    public boolean hasTopologyNeedSchedule() {
        return (!this.pendingQueue.isEmpty() && (this.pendingQueue.size() - this.attemptedQueue.size()) > 0);
    }

    @Override
    public int hashCode() {
        return this.userId.hashCode();
    }

    @Override
    public String toString() {
        return this.userId;
    }

    public String getDetailedInfo() {
        String ret = "\nUser: " + this.userId;
        ret += "\n - " + " Resource Pool: " + this.resourcePool;
        ret += "\n - " + " Running Queue: " + this.runningQueue;
        ret += "\n - " + " Pending Queue: " + this.pendingQueue;
        ret += "\n - " + " Attempted Queue: " + this.attemptedQueue;
        ret += "\n - " + " CPU Used: " + this.getCPUResourceUsedByUser() + " CPU guaranteed: " + this.getCPUResourceGuaranteed();
        ret += "\n - " + " Memory Used: " + this.getMemoryResourceUsedByUser() + " Memory guaranteed: " + this.getMemoryResourceGuaranteed();
        ret += "\n - " + " % Resource Guarantee Used: \n -- CPU: " + this.getCPUResourcePoolUtilization()
                + " Memory: " + this.getMemoryResourcePoolUtilization() + " Average: " + this.getResourcePoolAverageUtilization();
        return ret;
    }

    public static String getResourcePoolAverageUtilizationForUsers(Collection<User> users) {
        String ret = "";
        for(User user : users) {
            ret += user.getId() + " - " + user.getResourcePoolAverageUtilization() + " ";
        }
        return ret;
    }

    /**
     * Comparator that sorts topologies by priority and then by submission time
     */
    static class PQsortByPriorityAndSubmittionTime implements Comparator<TopologyDetails> {

        public int compare(TopologyDetails topo1, TopologyDetails topo2) {
            if (topo1.getTopologyPriority() > topo2.getTopologyPriority()) {
                return 1;
            } else if (topo1.getTopologyPriority() < topo2.getTopologyPriority()) {
                return -1;
            } else {
                if (topo1.getUpTime() > topo2.getUpTime()) {
                    return -1;
                } else if (topo1.getUpTime() < topo2.getUpTime()) {
                    return 1;
                } else {
                    return 0;
                }
            }
        }
    }
}
