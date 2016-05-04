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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

//TODO: improve this by maintaining slot -> executors as well for more efficient operations
public class SchedulerAssignmentImpl implements SchedulerAssignment {
    /**
     * topology-id this assignment is for.
     */
    String topologyId;
    /**
     * assignment detail, a mapping from executor to <code>WorkerSlot</code>
     */
    Map<ExecutorDetails, WorkerSlot> executorToSlot;
    
    public SchedulerAssignmentImpl(String topologyId, Map<ExecutorDetails, WorkerSlot> executorToSlots) {
        this.topologyId = topologyId;
        this.executorToSlot = new HashMap<>(0);
        if (executorToSlots != null) {
            this.executorToSlot.putAll(executorToSlots);
        }
    }

    @Override
    public Set<WorkerSlot> getSlots() {
        return new HashSet<>(executorToSlot.values());
    }    
    
    /**
     * Assign the slot to executors.
     */
    public void assign(WorkerSlot slot, Collection<ExecutorDetails> executors) {
        for (ExecutorDetails executor : executors) {
            this.executorToSlot.put(executor, slot);
        }
    }
    
    /**
     * Release the slot occupied by this assignment.
     */
    public void unassignBySlot(WorkerSlot slot) {
        List<ExecutorDetails> executors = new ArrayList<>();
        for (ExecutorDetails executor : this.executorToSlot.keySet()) {
            WorkerSlot ws = this.executorToSlot.get(executor);
            if (ws.equals(slot)) {
                executors.add(executor);
            }
        }
        
        // remove
        for (ExecutorDetails executor : executors) {
            this.executorToSlot.remove(executor);
        }
    }

    /**
     * @param slot
     * @return true if slot is occupied by this assignment
     */
    public boolean isSlotOccupied(WorkerSlot slot) {
        return this.executorToSlot.containsValue(slot);
    }

    public boolean isExecutorAssigned(ExecutorDetails executor) {
        return this.executorToSlot.containsKey(executor);
    }
    
    public String getTopologyId() {
        return this.topologyId;
    }

    public Map<ExecutorDetails, WorkerSlot> getExecutorToSlot() {
        return this.executorToSlot;
    }

    /**
     * @return the executors covered by this assignments
     */
    public Set<ExecutorDetails> getExecutors() {
        return this.executorToSlot.keySet();
    }

    public Map<WorkerSlot, Collection<ExecutorDetails>> getSlotToExecutors() {
        Map<WorkerSlot, Collection<ExecutorDetails>> ret = new HashMap<WorkerSlot, Collection<ExecutorDetails>>();
        for (Map.Entry<ExecutorDetails, WorkerSlot> entry : executorToSlot.entrySet()) {
            ExecutorDetails exec = entry.getKey();
            WorkerSlot ws = entry.getValue();
            if (!ret.containsKey(ws)) {
                ret.put(ws, new LinkedList<ExecutorDetails>());
            }
            ret.get(ws).add(exec);
        }
        return ret;
    }
}
