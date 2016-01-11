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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.TopologyDetails;

/**
 * All of the machines that currently have nothing assigned to them
 */
public class FreePool extends NodePool {
  private static final Logger LOG = LoggerFactory.getLogger(FreePool.class);
  private Set<Node> _nodes = new HashSet<>();
  private int _totalSlots = 0;

  @Override
  public void init(Cluster cluster, Map<String, Node> nodeIdToNode) {
    super.init(cluster, nodeIdToNode);
    for (Node n: nodeIdToNode.values()) {
      if(n.isTotallyFree() && n.isAlive()) {
        _nodes.add(n);
        _totalSlots += n.totalSlotsFree();
      }
    }
    LOG.debug("Found {} nodes with {} slots", _nodes.size(), _totalSlots);
  }
  
  @Override
  public void addTopology(TopologyDetails td) {
    throw new IllegalArgumentException("The free pool cannot run any topologies");
  }

  @Override
  public boolean canAdd(TopologyDetails td) {
    // The free pool never has anything running
    return false;
  }
  
  @Override
  public Collection<Node> takeNodes(int nodesNeeded) {
    HashSet<Node> ret = new HashSet<>();
    Iterator<Node> it = _nodes.iterator();
    while (it.hasNext() && nodesNeeded > ret.size()) {
      Node n = it.next();
      ret.add(n);
      _totalSlots -= n.totalSlotsFree();
      it.remove();
    }
    return ret;
  }
  
  @Override
  public int nodesAvailable() {
    return _nodes.size();
  }

  @Override
  public int slotsAvailable() {
    return _totalSlots;
  }

  @Override
  public Collection<Node> takeNodesBySlots(int slotsNeeded) {
    HashSet<Node> ret = new HashSet<>();
    Iterator<Node> it = _nodes.iterator();
    while (it.hasNext() && slotsNeeded > 0) {
      Node n = it.next();
      ret.add(n);
      _totalSlots -= n.totalSlotsFree();
      slotsNeeded -= n.totalSlotsFree();
      it.remove();
    }
    return ret;
  }
  
  @Override
  public NodeAndSlotCounts getNodeAndSlotCountIfSlotsWereTaken(int slotsNeeded) {
    int slotsFound = 0;
    int nodesFound = 0;
    Iterator<Node> it = _nodes.iterator();
    while (it.hasNext() && slotsNeeded > 0) {
      Node n = it.next();
      nodesFound++;
      int totalSlots = n.totalSlots();
      slotsFound += totalSlots;
      slotsNeeded -= totalSlots;
    }
    return new NodeAndSlotCounts(nodesFound, slotsFound);
  }

  @Override
  public void scheduleAsNeeded(NodePool... lesserPools) {
    //No topologies running so NOOP
  }
  
  @Override
  public String toString() {
    return "FreePool of "+_nodes.size()+" nodes with "+_totalSlots+" slots";
  }
}
