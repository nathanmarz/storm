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

package org.apache.storm.scheduler.multitenant;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.WorkerSlot;

/**
 * Represents a single node in the cluster.
 */
public class Node {
  private static final Logger LOG = LoggerFactory.getLogger(Node.class);
  private Map<String, Set<WorkerSlot>> _topIdToUsedSlots = new HashMap<>();
  private Set<WorkerSlot> _freeSlots = new HashSet<>();
  private final String _nodeId;
  private boolean _isAlive;
  
  public Node(String nodeId, Set<Integer> allPorts, boolean isAlive) {
    _nodeId = nodeId;
    _isAlive = isAlive;
    if (_isAlive && allPorts != null) {
      for (int port: allPorts) {
        _freeSlots.add(new WorkerSlot(_nodeId, port));
      }
    }
  }

  public String getId() {
    return _nodeId;
  }
  
  public boolean isAlive() {
    return _isAlive;
  }
  
  /**
   * @return a collection of the topology ids currently running on this node
   */
  public Collection<String> getRunningTopologies() {
    return _topIdToUsedSlots.keySet();
  }
  
  public boolean isTotallyFree() {
    return _topIdToUsedSlots.isEmpty();
  }
  
  public int totalSlotsFree() {
    return _freeSlots.size();
  }
  
  public int totalSlotsUsed() {
    int total = 0;
    for (Set<WorkerSlot> slots: _topIdToUsedSlots.values()) {
      total += slots.size();
    }
    return total;
  }
  
  public int totalSlots() {
    return totalSlotsFree() + totalSlotsUsed();
  }
  
  public int totalSlotsUsed(String topId) {
    int total = 0;
    Set<WorkerSlot> slots = _topIdToUsedSlots.get(topId);
    if (slots != null) {
      total = slots.size();
    }
    return total;
  }

  private void validateSlot(WorkerSlot ws) {
    if (!_nodeId.equals(ws.getNodeId())) {
      throw new IllegalArgumentException(
          "Trying to add a slot to the wrong node " + ws + 
          " is not a part of " + _nodeId);
    }
  }
 
  private void addOrphanedSlot(WorkerSlot ws) {
    if (_isAlive) {
      throw new IllegalArgumentException("Orphaned Slots " +
        "only are allowed on dead nodes.");
    }
    validateSlot(ws);
    if (_freeSlots.contains(ws)) {
      return;
    }
    for (Set<WorkerSlot> used: _topIdToUsedSlots.values()) {
      if (used.contains(ws)) {
        return;
      }
    }
    _freeSlots.add(ws);
  }
 
  boolean assignInternal(WorkerSlot ws, String topId, boolean dontThrow) {
    validateSlot(ws);
    if (!_freeSlots.remove(ws)) {
      for (Entry<String, Set<WorkerSlot>> topologySetEntry : _topIdToUsedSlots.entrySet()) {
        if (topologySetEntry.getValue().contains(ws)) {
          if (dontThrow) {
            LOG.warn("Worker slot [" + ws + "] can't be assigned to " + topId +
                    ". Its already assigned to " + topologySetEntry.getKey() + ".");
            return true;
          }
          throw new IllegalStateException("Worker slot [" + ws + "] can't be assigned to "
                  + topId + ". Its already assigned to " + topologySetEntry.getKey() + ".");
        }
      }
      LOG.warn("Adding Worker slot [" + ws + "] that was not reported in the supervisor heartbeats," +
              " but the worker is already running for topology " + topId + ".");
    }
    Set<WorkerSlot> usedSlots = _topIdToUsedSlots.get(topId);
    if (usedSlots == null) {
      usedSlots = new HashSet<>();
      _topIdToUsedSlots.put(topId, usedSlots);
    }
    usedSlots.add(ws);
    return false;
  }
  
  /**
   * Free all slots on this node.  This will update the Cluster too.
   * @param cluster the cluster to be updated
   */
  public void freeAllSlots(Cluster cluster) {
    if (!_isAlive) {
      LOG.warn("Freeing all slots on a dead node {} ",_nodeId);
    } 
    for (Entry<String, Set<WorkerSlot>> entry : _topIdToUsedSlots.entrySet()) {
      cluster.freeSlots(entry.getValue());
      if (_isAlive) {
        _freeSlots.addAll(entry.getValue());
      }
    }
    _topIdToUsedSlots = new HashMap<>();
  }
  
  /**
   * Frees a single slot in this node
   * @param ws the slot to free
   * @param cluster the cluster to update
   */
  public void free(WorkerSlot ws, Cluster cluster, boolean forceFree) {
    if (_freeSlots.contains(ws)) return;
    boolean wasFound = false;
    for (Entry<String, Set<WorkerSlot>> entry : _topIdToUsedSlots.entrySet()) {
      Set<WorkerSlot> slots = entry.getValue();
      if (slots.remove(ws)) {
        cluster.freeSlot(ws);
        if (_isAlive) {
          _freeSlots.add(ws);
        }
        wasFound = true;
      }
    }
    if(!wasFound)
    {
      if(forceFree)
      {
        LOG.info("Forcefully freeing the " + ws);
        cluster.freeSlot(ws);
        _freeSlots.add(ws);
      } else {
        throw new IllegalArgumentException("Tried to free a slot that was not" +
              " part of this node " + _nodeId);
      }
    }
  }
   
  /**
   * Frees all the slots for a topology.
   * @param topId the topology to free slots for
   * @param cluster the cluster to update
   */
  public void freeTopology(String topId, Cluster cluster) {
    Set<WorkerSlot> slots = _topIdToUsedSlots.get(topId);
    if (slots == null || slots.isEmpty()) return;
    for (WorkerSlot ws : slots) {
      cluster.freeSlot(ws);
      if (_isAlive) {
        _freeSlots.add(ws);
      }
    }
    _topIdToUsedSlots.remove(topId);
  }
 
  /**
   * Assign a free slot on the node to the following topology and executors.
   * This will update the cluster too.
   * @param topId the topology to assign a free slot to.
   * @param executors the executors to run in that slot.
   * @param cluster the cluster to be updated
   */
  public void assign(String topId, Collection<ExecutorDetails> executors, 
      Cluster cluster) {
    if (!_isAlive) {
      throw new IllegalStateException("Trying to adding to a dead node " + _nodeId);
    }
    if (_freeSlots.isEmpty()) {
      throw new IllegalStateException("Trying to assign to a full node " + _nodeId);
    }
    if (executors.size() == 0) {
      LOG.warn("Trying to assign nothing from " + topId + " to " + _nodeId + " (Ignored)");
    } else {
      WorkerSlot slot = _freeSlots.iterator().next();
      cluster.assign(slot, topId, executors);
      assignInternal(slot, topId, false);
    }
  }
  
  @Override
  public boolean equals(Object other) {
      return other instanceof Node && _nodeId.equals(((Node) other)._nodeId);
  }
  
  @Override
  public int hashCode() {
    return _nodeId.hashCode();
  }
  
  @Override
  public String toString() {
    return "Node: " + _nodeId;
  }

  public static int countSlotsUsed(String topId, Collection<Node> nodes) {
    int total = 0;
    for (Node n: nodes) {
      total += n.totalSlotsUsed(topId);
    }
    return total;
  }
  
  public static int countSlotsUsed(Collection<Node> nodes) {
    int total = 0;
    for (Node n: nodes) {
      total += n.totalSlotsUsed();
    }
    return total;
  }
  
  public static int countFreeSlotsAlive(Collection<Node> nodes) {
    int total = 0;
    for (Node n: nodes) {
      if (n.isAlive()) {
        total += n.totalSlotsFree();
      }
    }
    return total;
  }
  
  public static int countTotalSlotsAlive(Collection<Node> nodes) {
    int total = 0;
    for (Node n: nodes) {
      if (n.isAlive()) {
        total += n.totalSlots();
      }
    }
    return total;
  }
  
  public static Map<String, Node> getAllNodesFrom(Cluster cluster) {
    Map<String, Node> nodeIdToNode = new HashMap<>();
    for (SupervisorDetails sup : cluster.getSupervisors().values()) {
      //Node ID and supervisor ID are the same.
      String id = sup.getId();
      boolean isAlive = !cluster.isBlackListed(id);
      LOG.debug("Found a {} Node {} {}",
              isAlive? "living":"dead", id, sup.getAllPorts());
      nodeIdToNode.put(id, new Node(id, sup.getAllPorts(), isAlive));
    }
    
    for (Entry<String, SchedulerAssignment> entry : cluster.getAssignments().entrySet()) {
      String topId = entry.getValue().getTopologyId();
      for (WorkerSlot ws: entry.getValue().getSlots()) {
        String id = ws.getNodeId();
        Node node = nodeIdToNode.get(id);
        if (node == null) {
          LOG.debug("Found an assigned slot on a dead supervisor {}", ws);
          node = new Node(id, null, false);
          nodeIdToNode.put(id, node);
        }
        if (!node.isAlive()) {
          //The supervisor on the node down so add an orphaned slot to hold the unsupervised worker 
          node.addOrphanedSlot(ws);
        }
        if (node.assignInternal(ws, topId, true)) {
          LOG.warn("Bad scheduling state for topology [" + topId+ "], the slot " +
                  ws + " assigned to multiple workers, un-assigning everything...");
          node.free(ws, cluster, true);
        }
      }
    }
    
    return nodeIdToNode;
  }
  
  /**
   * Used to sort a list of nodes so the node with the most free slots comes
   * first.
   */
  public static final Comparator<Node> FREE_NODE_COMPARATOR_DEC = new Comparator<Node>() {
    @Override
    public int compare(Node o1, Node o2) {
      return o1.totalSlotsUsed() - o2.totalSlotsUsed();
    }
  };
}
