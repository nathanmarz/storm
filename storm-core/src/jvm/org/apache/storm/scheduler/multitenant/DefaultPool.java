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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;

/**
 * A pool of machines that anyone can use, but topologies are not isolated
 */
public class DefaultPool extends NodePool {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultPool.class);
  private Set<Node> _nodes = new HashSet<>();
  private HashMap<String, TopologyDetails> _tds = new HashMap<>();
  
  @Override
  public void addTopology(TopologyDetails td) {
    String topId = td.getId();
    LOG.debug("Adding in Topology {}", topId);
    _tds.put(topId, td);
    SchedulerAssignment assignment = _cluster.getAssignmentById(topId);
    if (assignment != null) {
      for (WorkerSlot ws: assignment.getSlots()) {
        Node n = _nodeIdToNode.get(ws.getNodeId());
        _nodes.add(n);
      }
    }
  }

  @Override
  public boolean canAdd(TopologyDetails td) {
    return true;
  }

  @Override
  public Collection<Node> takeNodes(int nodesNeeded) {
    HashSet<Node> ret = new HashSet<>();
    LinkedList<Node> sortedNodes = new LinkedList<>(_nodes);
    Collections.sort(sortedNodes, Node.FREE_NODE_COMPARATOR_DEC);
    for (Node n: sortedNodes) {
      if (nodesNeeded <= ret.size()) {
        break;
      }
      if (n.isAlive()) {
        n.freeAllSlots(_cluster);
        _nodes.remove(n);
        ret.add(n);
      }
    }
    return ret;
  }
  
  @Override
  public int nodesAvailable() {
    int total = 0;
    for (Node n: _nodes) {
      if (n.isAlive()) total++;
    }
    return total;
  }
  
  @Override
  public int slotsAvailable() {
    return Node.countTotalSlotsAlive(_nodes);
  }

  @Override
  public NodeAndSlotCounts getNodeAndSlotCountIfSlotsWereTaken(int slotsNeeded) {
    int nodesFound = 0;
    int slotsFound = 0;
    LinkedList<Node> sortedNodes = new LinkedList<>(_nodes);
    Collections.sort(sortedNodes, Node.FREE_NODE_COMPARATOR_DEC);
    for (Node n: sortedNodes) {
      if (slotsNeeded <= 0) {
        break;
      }
      if (n.isAlive()) {
        nodesFound++;
        int totalSlotsFree = n.totalSlots();
        slotsFound += totalSlotsFree;
        slotsNeeded -= totalSlotsFree;
      }
    }
    return new NodeAndSlotCounts(nodesFound, slotsFound);
  }
  
  @Override
  public Collection<Node> takeNodesBySlots(int slotsNeeded) {
    HashSet<Node> ret = new HashSet<>();
    LinkedList<Node> sortedNodes = new LinkedList<>(_nodes);
    Collections.sort(sortedNodes, Node.FREE_NODE_COMPARATOR_DEC);
    for (Node n: sortedNodes) {
      if (slotsNeeded <= 0) {
        break;
      }
      if (n.isAlive()) {
        n.freeAllSlots(_cluster);
        _nodes.remove(n);
        ret.add(n);
        slotsNeeded -= n.totalSlotsFree();
      }
    }
    return ret;
  }

  @Override
  public void scheduleAsNeeded(NodePool... lesserPools) {
    for (TopologyDetails td : _tds.values()) {
      String topId = td.getId();
      if (_cluster.needsScheduling(td)) {
        LOG.debug("Scheduling topology {}",topId);
        int totalTasks = td.getExecutors().size();
        int origRequest = td.getNumWorkers();
        int slotsRequested = Math.min(totalTasks, origRequest);
        int slotsUsed = Node.countSlotsUsed(topId, _nodes);
        int slotsFree = Node.countFreeSlotsAlive(_nodes);
        //Check to see if we have enough slots before trying to get them
        int slotsAvailable = 0;
        if (slotsRequested > slotsFree) {
          slotsAvailable = NodePool.slotsAvailable(lesserPools);
        }
        int slotsToUse = Math.min(slotsRequested - slotsUsed, slotsFree + slotsAvailable);
        int executorsNotRunning = _cluster.getUnassignedExecutors(td).size();
        LOG.debug("Slots... requested {} used {} free {} available {} to be used {}, executors not running {}",
                slotsRequested, slotsUsed, slotsFree, slotsAvailable, slotsToUse, executorsNotRunning);
        if (slotsToUse <= 0) {
          if (executorsNotRunning > 0) {
            _cluster.setStatus(topId,"Not fully scheduled (No free slots in default pool) "+executorsNotRunning+" executors not scheduled");
          } else {
            if (slotsUsed < slotsRequested) {
              _cluster.setStatus(topId,"Running with fewer slots than requested ("+slotsUsed+"/"+origRequest+")");
            } else { //slotsUsed < origRequest
              _cluster.setStatus(topId,"Fully Scheduled (requested "+origRequest+" slots, but could only use "+slotsUsed+")");
            }
          }
          continue;
        }

        int slotsNeeded = slotsToUse - slotsFree;
        if (slotsNeeded > 0) {
          _nodes.addAll(NodePool.takeNodesBySlot(slotsNeeded, lesserPools));
        }

        if (executorsNotRunning <= 0) {
          //There are free slots that we can take advantage of now.
          for (Node n: _nodes) {
            n.freeTopology(topId, _cluster); 
          }
          slotsFree = Node.countFreeSlotsAlive(_nodes);
          slotsToUse = Math.min(slotsRequested, slotsFree);
        }
        
        RoundRobinSlotScheduler slotSched = 
          new RoundRobinSlotScheduler(td, slotsToUse, _cluster);
        
        LinkedList<Node> nodes = new LinkedList<>(_nodes);
        while (true) {
          Node n;
          do {
            if (nodes.isEmpty()) {
              throw new IllegalStateException("This should not happen, we" +
              " messed up and did not get enough slots");
            }
            n = nodes.peekFirst();
            if (n.totalSlotsFree() == 0) {
              nodes.remove();
              n = null;
            }
          } while (n == null);
          if (!slotSched.assignSlotTo(n)) {
            break;
          }
        }
        int afterSchedSlotsUsed = Node.countSlotsUsed(topId, _nodes);
        if (afterSchedSlotsUsed < slotsRequested) {
          _cluster.setStatus(topId,"Running with fewer slots than requested ("+afterSchedSlotsUsed+"/"+origRequest+")");
        } else if (afterSchedSlotsUsed < origRequest) {
          _cluster.setStatus(topId,"Fully Scheduled (requested "+origRequest+" slots, but could only use "+afterSchedSlotsUsed+")");
        } else {
          _cluster.setStatus(topId,"Fully Scheduled");
        }
      } else {
        _cluster.setStatus(topId,"Fully Scheduled");
      }
    }
  }
  
  @Override
  public String toString() {
    return "DefaultPool  " + _nodes.size() + " nodes " + _tds.size() + " topologies";
  }
}
