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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class EvenScheduler implements IScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(EvenScheduler.class);

    public static List<WorkerSlot> sortSlots(List<WorkerSlot> availableSlots) {
        //For example, we have a three nodes(supervisor1, supervisor2, supervisor3) cluster:
        //slots before sort:
        //supervisor1:6700, supervisor1:6701,
        //supervisor2:6700, supervisor2:6701, supervisor2:6702,
        //supervisor3:6700, supervisor3:6703, supervisor3:6702, supervisor3:6701
        //slots after sort:
        //supervisor3:6700, supervisor2:6700, supervisor1:6700,
        //supervisor3:6701, supervisor2:6701, supervisor1:6701,
        //supervisor3:6702, supervisor2:6702,
        //supervisor3:6703

        if (availableSlots != null && availableSlots.size() > 0) {
            // group by node
            Map<String, List<WorkerSlot>> slotGroups = new TreeMap<String, List<WorkerSlot>>();
            for (WorkerSlot slot : availableSlots) {
                String node = slot.getNodeId();
                List<WorkerSlot> slots = null;
                if(slotGroups.containsKey(node)){
                   slots = slotGroups.get(node);
                }else{
                   slots = new ArrayList<WorkerSlot>();
                   slotGroups.put(node, slots);
                }
                slots.add(slot);
            }

            // sort by port: from small to large
            for (List<WorkerSlot> slots : slotGroups.values()) {
                Collections.sort(slots, new Comparator<WorkerSlot>() {
                    @Override
                    public int compare(WorkerSlot o1, WorkerSlot o2) {
                        return o1.getPort() - o2.getPort();
                    }
                });
            }

            // sort by available slots size: from large to small
            List<List<WorkerSlot>> list = new ArrayList<List<WorkerSlot>>(slotGroups.values());
            Collections.sort(list, new Comparator<List<WorkerSlot>>() {
                @Override
                public int compare(List<WorkerSlot> o1, List<WorkerSlot> o2) {
                    return o2.size() - o1.size();
                }
            });

            return Utils.interleaveAll(list);
        }

        return null;
    }

    public static Map<WorkerSlot, List<ExecutorDetails>> getAliveAssignedWorkerSlotExecutors(Cluster cluster, String topologyId) {
        SchedulerAssignment existingAssignment = cluster.getAssignmentById(topologyId);
        Map<ExecutorDetails, WorkerSlot> executorToSlot = null;
        if (existingAssignment != null) {
            executorToSlot = existingAssignment.getExecutorToSlot();
        }

        return Utils.reverseMap(executorToSlot);
    }

    private static Map<ExecutorDetails, WorkerSlot> scheduleTopology(TopologyDetails topology, Cluster cluster) {
        List<WorkerSlot> availableSlots = cluster.getAvailableSlots();
        Set<ExecutorDetails> allExecutors = topology.getExecutors();
        Map<WorkerSlot, List<ExecutorDetails>> aliveAssigned = getAliveAssignedWorkerSlotExecutors(cluster, topology.getId());
        int totalSlotsToUse = Math.min(topology.getNumWorkers(), availableSlots.size() + aliveAssigned.size());

        List<WorkerSlot> sortedList = sortSlots(availableSlots);
        if (sortedList == null || sortedList.size() < (totalSlotsToUse - aliveAssigned.size())) {
            LOG.error("Available slots are not enough for topology: {}", topology.getName());
            return new HashMap<ExecutorDetails, WorkerSlot>();
        }

        List<WorkerSlot> reassignSlots = sortedList.subList(0, totalSlotsToUse - aliveAssigned.size());
        Set<ExecutorDetails> aliveExecutors = new HashSet<ExecutorDetails>();
        for (List<ExecutorDetails> list : aliveAssigned.values()) {
            aliveExecutors.addAll(list);
        }
        Set<ExecutorDetails> reassignExecutors = Sets.difference(allExecutors, aliveExecutors);

        Map<ExecutorDetails, WorkerSlot> reassignment = new HashMap<ExecutorDetails, WorkerSlot>();
        if (reassignSlots.size() == 0) {
            return reassignment;
        }

        List<ExecutorDetails> executors = new ArrayList<ExecutorDetails>(reassignExecutors);
        Collections.sort(executors, new Comparator<ExecutorDetails>() {
            @Override
            public int compare(ExecutorDetails o1, ExecutorDetails o2) {
                return o1.getStartTask() - o2.getStartTask();
            }
        });

        for (int i = 0; i < executors.size(); i++) {
            reassignment.put(executors.get(i), reassignSlots.get(i % reassignSlots.size()));
        }

        if (reassignment.size() != 0) {
            LOG.info("Available slots: {}", availableSlots.toString());
        }
        return reassignment;
    }

    public static void scheduleTopologiesEvenly(Topologies topologies, Cluster cluster) {
        List<TopologyDetails> needsSchedulingTopologies = cluster.needsSchedulingTopologies(topologies);
        for (TopologyDetails topology : needsSchedulingTopologies) {
            String topologyId = topology.getId();
            Map<ExecutorDetails, WorkerSlot> newAssignment = scheduleTopology(topology, cluster);
            Map<WorkerSlot, List<ExecutorDetails>> nodePortToExecutors = Utils.reverseMap(newAssignment);

            for (Map.Entry<WorkerSlot, List<ExecutorDetails>> entry : nodePortToExecutors.entrySet()) {
                WorkerSlot nodePort = entry.getKey();
                List<ExecutorDetails> executors = entry.getValue();
                cluster.assign(nodePort, topologyId, executors);
            }
        }
    }

    @Override
    public void prepare(Map conf) {
        //noop
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        scheduleTopologiesEvenly(topologies, cluster);
    }

}
