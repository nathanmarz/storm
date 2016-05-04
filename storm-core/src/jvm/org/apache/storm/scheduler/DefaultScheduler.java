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
package org.apache.storm.scheduler;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.storm.utils.Utils;

public class DefaultScheduler implements IScheduler {

    private static Set<WorkerSlot> badSlots(Map<WorkerSlot, List<ExecutorDetails>> existingSlots, int numExecutors, int numWorkers) {
        if (numWorkers != 0) {
            Map<Integer, Integer> distribution = Utils.integerDivided(numExecutors, numWorkers);
            Set<WorkerSlot> slots = new HashSet<WorkerSlot>();

            for (Entry<WorkerSlot, List<ExecutorDetails>> entry : existingSlots.entrySet()) {
                Integer executorCount = entry.getValue().size();
                Integer workerCount = distribution.get(executorCount);
                if (workerCount != null && workerCount > 0) {
                    slots.add(entry.getKey());
                    workerCount--;
                    distribution.put(executorCount, workerCount);
                }
            }

            for (WorkerSlot slot : slots) {
                existingSlots.remove(slot);                
            }

            return existingSlots.keySet();
        }

        return null;
    }

    public static Set<WorkerSlot> slotsCanReassign(Cluster cluster, Set<WorkerSlot> slots) {
        Set<WorkerSlot> result = new HashSet<WorkerSlot>();
        for (WorkerSlot slot : slots) {
            if (!cluster.isBlackListed(slot.getNodeId())) {
                SupervisorDetails supervisor = cluster.getSupervisorById(slot.getNodeId());
                if (supervisor != null) {
                    Set<Integer> ports = supervisor.getAllPorts();
                    if (ports != null && ports.contains(slot.getPort())) {
                        result.add(slot);
                    }
                }
            }
        }
        return result;
    }

    public static void defaultSchedule(Topologies topologies, Cluster cluster) {
        List<TopologyDetails> needsSchedulingTopologies = cluster.needsSchedulingTopologies(topologies);
        for (TopologyDetails topology : needsSchedulingTopologies) {
            List<WorkerSlot> availableSlots = cluster.getAvailableSlots();
            Set<ExecutorDetails> allExecutors = topology.getExecutors();

            Map<WorkerSlot, List<ExecutorDetails>> aliveAssigned = EvenScheduler.getAliveAssignedWorkerSlotExecutors(cluster, topology.getId());
            Set<ExecutorDetails> aliveExecutors = new HashSet<ExecutorDetails>();
            for (List<ExecutorDetails> list : aliveAssigned.values()) {
                aliveExecutors.addAll(list);
            }

            Set<WorkerSlot> canReassignSlots = slotsCanReassign(cluster, aliveAssigned.keySet());
            int totalSlotsToUse = Math.min(topology.getNumWorkers(), canReassignSlots.size() + availableSlots.size());

            Set<WorkerSlot> badSlots = null;
            if (totalSlotsToUse > aliveAssigned.size() || !allExecutors.equals(aliveExecutors)) {
                badSlots = badSlots(aliveAssigned, allExecutors.size(), totalSlotsToUse);                
            }
            if (badSlots != null) {
                cluster.freeSlots(badSlots);                
            }

            Map<String, TopologyDetails> _topologies = new HashMap<String, TopologyDetails>();
            _topologies.put(topology.getId(), topology);
            EvenScheduler.scheduleTopologiesEvenly(new Topologies(_topologies), cluster);
        }
    }

    @Override
    public void prepare(Map conf) {
        //noop
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        defaultSchedule(topologies, cluster);
    }

}
