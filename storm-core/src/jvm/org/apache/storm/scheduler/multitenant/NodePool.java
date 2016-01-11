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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.Config;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;

/**
 * A pool of nodes that can be used to run topologies.
 */
public abstract class NodePool {
  protected Cluster _cluster;
  protected Map<String, Node> _nodeIdToNode;
  
  public static class NodeAndSlotCounts {
    public final int _nodes;
    public final int _slots;
    
    public NodeAndSlotCounts(int nodes, int slots) {
      _nodes = nodes;
      _slots = slots;
    }
  }

  /**
   * Place executors into slots in a round robin way, taking into account
   * component spreading among different hosts.
   */
  public static class RoundRobinSlotScheduler {
    private Map<String,Set<String>> _nodeToComps;
    private HashMap<String, List<ExecutorDetails>> _spreadToSchedule;
    private LinkedList<Set<ExecutorDetails>> _slots;
    private Set<ExecutorDetails> _lastSlot;
    private Cluster _cluster;
    private String _topId;
    
    /**
     * Create a new scheduler for a given topology
     * @param td the topology to schedule
     * @param slotsToUse the number of slots to use for the executors left to 
     * schedule.
     * @param cluster the cluster to schedule this on. 
     */
    public RoundRobinSlotScheduler(TopologyDetails td, int slotsToUse, 
        Cluster cluster) {
      _topId = td.getId();
      _cluster = cluster;
      
      Map<ExecutorDetails, String> execToComp = td.getExecutorToComponent();
      SchedulerAssignment assignment = _cluster.getAssignmentById(_topId);
      _nodeToComps = new HashMap<>();

      if (assignment != null) {
        Map<ExecutorDetails, WorkerSlot> execToSlot = assignment.getExecutorToSlot();
        
        for (Entry<ExecutorDetails, WorkerSlot> entry: execToSlot.entrySet()) {
          String nodeId = entry.getValue().getNodeId();
          Set<String> comps = _nodeToComps.get(nodeId);
          if (comps == null) {
            comps = new HashSet<>();
            _nodeToComps.put(nodeId, comps);
          }
          comps.add(execToComp.get(entry.getKey()));
        }
      }
      
      _spreadToSchedule = new HashMap<>();
      List<String> spreadComps = (List<String>)td.getConf().get(Config.TOPOLOGY_SPREAD_COMPONENTS);
      if (spreadComps != null) {
        for (String comp: spreadComps) {
          _spreadToSchedule.put(comp, new ArrayList<ExecutorDetails>());
        }
      }
      
      _slots = new LinkedList<>();
      for (int i = 0; i < slotsToUse; i++) {
        _slots.add(new HashSet<ExecutorDetails>());
      }

      int at = 0;
      for (Entry<String, List<ExecutorDetails>> entry: _cluster.getNeedsSchedulingComponentToExecutors(td).entrySet()) {
        LOG.debug("Scheduling for {}", entry.getKey());
        if (_spreadToSchedule.containsKey(entry.getKey())) {
          LOG.debug("Saving {} for spread...",entry.getKey());
          _spreadToSchedule.get(entry.getKey()).addAll(entry.getValue());
        } else {
          for (ExecutorDetails ed: entry.getValue()) {
            LOG.debug("Assigning {} {} to slot {}", entry.getKey(), ed, at);
            _slots.get(at).add(ed);
            at++;
            if (at >= _slots.size()) {
              at = 0;
            }
          }
        }
      }
      _lastSlot = _slots.get(_slots.size() - 1);
    }
    
    /**
     * Assign a slot to the given node.
     * @param n the node to assign a slot to.
     * @return true if there are more slots to assign else false.
     */
    public boolean assignSlotTo(Node n) {
      if (_slots.isEmpty()) {
        return false;
      }
      Set<ExecutorDetails> slot = _slots.pop();
      if (slot == _lastSlot) {
        //The last slot fill it up
        for (Entry<String, List<ExecutorDetails>> entry: _spreadToSchedule.entrySet()) {
          if (entry.getValue().size() > 0) {
            slot.addAll(entry.getValue());
          }
        }
      } else {
        String nodeId = n.getId();
        Set<String> nodeComps = _nodeToComps.get(nodeId);
        if (nodeComps == null) {
          nodeComps = new HashSet<>();
          _nodeToComps.put(nodeId, nodeComps);
        }
        for (Entry<String, List<ExecutorDetails>> entry: _spreadToSchedule.entrySet()) {
          if (entry.getValue().size() > 0) {
            String comp = entry.getKey();
            if (!nodeComps.contains(comp)) {
              nodeComps.add(comp);
              slot.add(entry.getValue().remove(0));
            }
          }
        }
      }
      n.assign(_topId, slot, _cluster);
      return !_slots.isEmpty();
    }
  }
  
  private static final Logger LOG = LoggerFactory.getLogger(NodePool.class);
  /**
   * Initialize the pool.
   * @param cluster the cluster
   * @param nodeIdToNode the mapping of node id to nodes
   */
  public void init(Cluster cluster, Map<String, Node> nodeIdToNode) {
    _cluster = cluster;
    _nodeIdToNode = nodeIdToNode;
  }
  
  /**
   * Add a topology to the pool
   * @param td the topology to add.
   */
  public abstract void addTopology(TopologyDetails td);
  
  /**
   * Check if this topology can be added to this pool
   * @param td the topology
   * @return true if it can else false
   */
  public abstract boolean canAdd(TopologyDetails td);
  
  /**
   * @return the number of nodes that are available to be taken
   */
  public abstract int slotsAvailable();
  
  /**
   * Take nodes from this pool that can fulfill possibly up to the
   * slotsNeeded
   * @param slotsNeeded the number of slots that are needed.
   * @return a Collection of nodes with the removed nodes in it.  
   * This may be empty, but should not be null.
   */
  public abstract Collection<Node> takeNodesBySlots(int slotsNeeded);

  /**
   * Get the number of nodes and slots this would provide to get the slots needed
   * @param slots the number of slots needed
   * @return the number of nodes and slots that would be returned.
   */
  public abstract NodeAndSlotCounts getNodeAndSlotCountIfSlotsWereTaken(int slots);
  
  /**
   * @return the number of nodes that are available to be taken
   */
  public abstract int nodesAvailable();
  
  /**
   * Take up to nodesNeeded from this pool
   * @param nodesNeeded the number of nodes that are needed.
   * @return a Collection of nodes with the removed nodes in it.  
   * This may be empty, but should not be null.
   */
  public abstract Collection<Node> takeNodes(int nodesNeeded);
  
  /**
   * Reschedule any topologies as needed.
   * @param lesserPools pools that may be used to steal nodes from.
   */
  public abstract void scheduleAsNeeded(NodePool ... lesserPools);
  
  public static int slotsAvailable(NodePool[] pools) {
    int slotsAvailable = 0;
    for (NodePool pool: pools) {
      slotsAvailable += pool.slotsAvailable();
    }
    return slotsAvailable;
  }
  
  public static int nodesAvailable(NodePool[] pools) {
    int nodesAvailable = 0;
    for (NodePool pool: pools) {
      nodesAvailable += pool.nodesAvailable();
    }
    return nodesAvailable;
  }
  
  public static Collection<Node> takeNodesBySlot(int slotsNeeded,NodePool[] pools) {
    LOG.debug("Trying to grab {} free slots from {}",slotsNeeded, pools);
    HashSet<Node> ret = new HashSet<>();
    for (NodePool pool: pools) {
      Collection<Node> got = pool.takeNodesBySlots(slotsNeeded);
      ret.addAll(got);
      slotsNeeded -= Node.countFreeSlotsAlive(got);
      LOG.debug("Got {} nodes so far need {} more slots",ret.size(),slotsNeeded);
      if (slotsNeeded <= 0) {
        break;
      }
    }
    return ret;
  }
  
  public static Collection<Node> takeNodes(int nodesNeeded,NodePool[] pools) {
    LOG.debug("Trying to grab {} free nodes from {}",nodesNeeded, pools);
    HashSet<Node> ret = new HashSet<>();
    for (NodePool pool: pools) {
      Collection<Node> got = pool.takeNodes(nodesNeeded);
      ret.addAll(got);
      nodesNeeded -= got.size();
      LOG.debug("Got {} nodes so far need {} more nodes", ret.size(), nodesNeeded);
      if (nodesNeeded <= 0) {
        break;
      }
    }
    return ret;
  }

  public static int getNodeCountIfSlotsWereTaken(int slots,NodePool[] pools) {
    LOG.debug("How many nodes to get {} slots from {}",slots, pools);
    int total = 0;
    for (NodePool pool: pools) {
      NodeAndSlotCounts ns = pool.getNodeAndSlotCountIfSlotsWereTaken(slots);
      total += ns._nodes;
      slots -= ns._slots;
      LOG.debug("Found {} nodes so far {} more slots needed", total, slots);
      if (slots <= 0) {
        break;
      }
    }    
    return total;
  }
}
