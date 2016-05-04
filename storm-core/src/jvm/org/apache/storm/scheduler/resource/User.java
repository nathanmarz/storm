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

import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.TopologyDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class User {
    private String userId;
    //Topologies yet to be scheduled sorted by priority for each user
    private TreeSet<TopologyDetails> pendingQueue = new TreeSet<TopologyDetails>(new PQsortByPriorityAndSubmittionTime());

    //Topologies yet to be scheduled sorted by priority for each user
    private TreeSet<TopologyDetails> runningQueue = new TreeSet<TopologyDetails>(new PQsortByPriorityAndSubmittionTime());

    //Topologies that was attempted to be scheduled but wasn't successful
    private TreeSet<TopologyDetails> attemptedQueue = new TreeSet<TopologyDetails>(new PQsortByPriorityAndSubmittionTime());

    //Topologies that was deemed to be invalid
    private TreeSet<TopologyDetails> invalidQueue = new TreeSet<TopologyDetails>(new PQsortByPriorityAndSubmittionTime());

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
        if (this.resourcePool.get("cpu") == null) {
            this.resourcePool.put("cpu", 0.0);
        }
        if (this.resourcePool.get("memory") == null) {
            this.resourcePool.put("memory", 0.0);
        }
    }

    /**
     * Copy Constructor
     */
    public User(User src) {
        this(src.userId, src.resourcePool);
        for (TopologyDetails topo : src.pendingQueue) {
            addTopologyToPendingQueue(topo);
        }
        for (TopologyDetails topo : src.runningQueue) {
            addTopologyToRunningQueue(topo);
        }
        for (TopologyDetails topo : src.attemptedQueue) {
            addTopologyToAttemptedQueue(topo);
        }
        for (TopologyDetails topo : src.invalidQueue) {
            addTopologyToInvalidQueue(topo);
        }
    }

    public String getId() {
        return this.userId;
    }

    public void addTopologyToPendingQueue(TopologyDetails topo, Cluster cluster) {
        this.pendingQueue.add(topo);
        if (cluster != null) {
            cluster.setStatus(topo.getId(), "Scheduling Pending");
        }
    }

    public void addTopologyToPendingQueue(TopologyDetails topo) {
        this.addTopologyToPendingQueue(topo, null);
    }

    public void addTopologyToRunningQueue(TopologyDetails topo, Cluster cluster) {
        this.runningQueue.add(topo);
        if (cluster != null) {
            cluster.setStatus(topo.getId(), "Fully Scheduled");
        }
    }

    public void addTopologyToRunningQueue(TopologyDetails topo) {
        this.addTopologyToRunningQueue(topo, null);
    }

    public Set<TopologyDetails> getTopologiesPending() {
        TreeSet<TopologyDetails> ret = new TreeSet<TopologyDetails>(new PQsortByPriorityAndSubmittionTime());
        ret.addAll(this.pendingQueue);
        return ret;
    }

    public void addTopologyToAttemptedQueue(TopologyDetails topo) {
        this.attemptedQueue.add(topo);
    }

    public void addTopologyToInvalidQueue(TopologyDetails topo) {
        this.invalidQueue.add(topo);
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

    public Set<TopologyDetails> getTopologiesInvalid() {
        TreeSet<TopologyDetails> ret = new TreeSet<TopologyDetails>(new PQsortByPriorityAndSubmittionTime());
        ret.addAll(this.invalidQueue);
        return ret;
    }

    public Map<String, Number> getResourcePool() {
        if (this.resourcePool != null) {
            return new HashMap<String, Number>(this.resourcePool);
        }
        return null;
    }

    public void moveTopoFromPendingToRunning(TopologyDetails topo, Cluster cluster) {
        moveTopology(topo, this.pendingQueue, "pending", this.runningQueue, "running");
        if (cluster != null) {
            cluster.setStatus(topo.getId(), "Fully Scheduled");
        }
    }

    public void moveTopoFromPendingToRunning(TopologyDetails topo) {
        this.moveTopoFromPendingToRunning(topo, null);
    }

    public void moveTopoFromPendingToAttempted(TopologyDetails topo, Cluster cluster) {
        moveTopology(topo, this.pendingQueue, "pending", this.attemptedQueue, "attempted");
        if (cluster != null) {
            cluster.setStatus(topo.getId(), "Scheduling Attempted but Failed");
        }
    }

    public void moveTopoFromPendingToAttempted(TopologyDetails topo) {
        this.moveTopoFromPendingToAttempted(topo, null);
    }

    public void moveTopoFromPendingToInvalid(TopologyDetails topo, Cluster cluster) {
        moveTopology(topo, this.pendingQueue, "pending", this.invalidQueue, "invalid");
        if (cluster != null) {
            cluster.setStatus(topo.getId(), "Scheduling Attempted but topology is invalid");
        }
    }

    public void moveTopoFromPendingToInvalid(TopologyDetails topo) {
        this.moveTopoFromPendingToInvalid(topo, null);
    }

    public void moveTopoFromRunningToPending(TopologyDetails topo, Cluster cluster) {
        moveTopology(topo, this.runningQueue, "running", this.pendingQueue, "pending");
        if (cluster != null) {
            cluster.setStatus(topo.getId(), "Scheduling Pending");
        }
    }

    public void moveTopoFromRunningToPending(TopologyDetails topo) {
        this.moveTopoFromRunningToPending(topo, null);
    }

    private void moveTopology(TopologyDetails topo, Set<TopologyDetails> src, String srcName, Set<TopologyDetails> dest, String destName) {
        if (topo == null) {
            return;
        }

        LOG.debug("For User {} Moving topo {} from {} to {}", this.userId, topo.getName(), srcName, destName);

        if (!src.contains(topo)) {
            LOG.warn("Topo {} not in User: {} {} queue!", topo.getName(), this.userId, srcName);
            return;
        }
        if (dest.contains(topo)) {
            LOG.warn("Topo {} already in in User: {} {} queue!", topo.getName(), this.userId, destName);
            return;
        }
        src.remove(topo);
        dest.add(topo);
    }

    public double getResourcePoolAverageUtilization() {
        Double cpuResourcePoolUtilization = this.getCPUResourcePoolUtilization();
        Double memoryResourcePoolUtilization = this.getMemoryResourcePoolUtilization();

        if (cpuResourcePoolUtilization != null && memoryResourcePoolUtilization != null) {
            //cannot be (cpuResourcePoolUtilization + memoryResourcePoolUtilization)/2
            //since memoryResourcePoolUtilization or cpuResourcePoolUtilization can be Double.MAX_VALUE
            //Should not return infinity in that case
            return ((cpuResourcePoolUtilization) / 2.0) + ((memoryResourcePoolUtilization) / 2.0);
        }
        return Double.MAX_VALUE;
    }

    public double getCPUResourcePoolUtilization() {
        Double cpuGuarantee = this.resourcePool.get("cpu");
        if (cpuGuarantee == null || cpuGuarantee == 0.0) {
            return Double.MAX_VALUE;
        }
        return this.getCPUResourceUsedByUser() / cpuGuarantee;
    }

    public double getMemoryResourcePoolUtilization() {
        Double memoryGuarantee = this.resourcePool.get("memory");
        if (memoryGuarantee == null || memoryGuarantee == 0.0) {
            return Double.MAX_VALUE;
        }
        return this.getMemoryResourceUsedByUser() / memoryGuarantee;
    }

    public double getCPUResourceUsedByUser() {
        double sum = 0.0;
        for (TopologyDetails topo : this.runningQueue) {
            sum += topo.getTotalRequestedCpu();
        }
        return sum;
    }

    public double getMemoryResourceUsedByUser() {
        double sum = 0.0;
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
            if (!this.attemptedQueue.contains(topo)) {
                return topo;
            }
        }
        return null;
    }

    public boolean hasTopologyNeedSchedule() {
        return (!this.pendingQueue.isEmpty());
    }

    public TopologyDetails getRunningTopologyWithLowestPriority() {
        if (this.runningQueue.isEmpty()) {
            return null;
        }
        return this.runningQueue.last();
    }

    @Override
    public int hashCode() {
        return this.userId.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof User)) {
            return false;
        }
        return this.getId().equals(((User) o).getId());
    }

    @Override
    public String toString() {
        return this.userId;
    }

    public String getDetailedInfo() {
        String ret = "\nUser: " + this.userId;
        ret += "\n - " + " Resource Pool: " + this.resourcePool;
        ret += "\n - " + " Running Queue: " + this.runningQueue + " size: " + this.runningQueue.size();
        ret += "\n - " + " Pending Queue: " + this.pendingQueue + " size: " + this.pendingQueue.size();
        ret += "\n - " + " Attempted Queue: " + this.attemptedQueue + " size: " + this.attemptedQueue.size();
        ret += "\n - " + " Invalid Queue: " + this.invalidQueue + " size: " + this.invalidQueue.size();
        ret += "\n - " + " CPU Used: " + this.getCPUResourceUsedByUser() + " CPU guaranteed: " + this.getCPUResourceGuaranteed();
        ret += "\n - " + " Memory Used: " + this.getMemoryResourceUsedByUser() + " Memory guaranteed: " + this.getMemoryResourceGuaranteed();
        ret += "\n - " + " % Resource Guarantee Used: \n -- CPU: " + this.getCPUResourcePoolUtilization()
                + " Memory: " + this.getMemoryResourcePoolUtilization() + " Average: " + this.getResourcePoolAverageUtilization();
        return ret;
    }

    public static String getResourcePoolAverageUtilizationForUsers(Collection<User> users) {
        String ret = "";
        for (User user : users) {
            ret += user.getId() + " - " + user.getResourcePoolAverageUtilization() + " ";
        }
        return ret;
    }

    /**
     * Comparator that sorts topologies by priority and then by submission time
     * First sort by Topology Priority, if there is a tie for topology priority, topology uptime is used to sort
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
                    return topo1.getId().compareTo(topo2.getId());
                }
            }
        }
    }
}
