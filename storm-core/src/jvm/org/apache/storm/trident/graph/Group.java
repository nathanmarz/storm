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
package org.apache.storm.trident.graph;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.jgrapht.DirectedGraph;
import org.apache.storm.trident.operation.ITridentResource;
import org.apache.storm.trident.planner.Node;
import org.apache.storm.trident.util.IndexedEdge;
import org.apache.storm.trident.util.TridentUtils;


public class Group implements ITridentResource {
    public final Set<Node> nodes = new HashSet<>();
    private final DirectedGraph<Node, IndexedEdge> graph;
    private final String id = UUID.randomUUID().toString();
    
    public Group(DirectedGraph graph, List<Node> nodes) {
        this.graph = graph;
        this.nodes.addAll(nodes);
    }
    
    public Group(DirectedGraph graph, Node n) {
        this(graph, Arrays.asList(n));
    }
    
    public Group(Group g1, Group g2) {
        this.graph = g1.graph;
        nodes.addAll(g1.nodes);
        nodes.addAll(g2.nodes);
    }
    
    public Set<Node> outgoingNodes() {
        Set<Node> ret = new HashSet<>();
        for(Node n: nodes) {
            ret.addAll(TridentUtils.getChildren(graph, n));
        }
        return ret;
    }
    
    public Set<Node> incomingNodes() {
        Set<Node> ret = new HashSet<>();
        for(Node n: nodes) {
            ret.addAll(TridentUtils.getParents(graph, n));
        }        
        return ret;        
    }

    @Override
    public Map<String, Number> getResources() {
        Map<String, Number> ret = new HashMap<>();
        for(Node n: nodes) {
            Map<String, Number> res = n.getResources();
            for(Map.Entry<String, Number> kv : res.entrySet()) {
                String key = kv.getKey();
                Number val = kv.getValue();
                if(ret.containsKey(key)) {
                    val = new Double(val.doubleValue() + ret.get(key).doubleValue());
                }
                ret.put(key, val);
            }
        }
        return ret;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return id.equals(((Group) o).id);
    }

    @Override
    public String toString() {
        return nodes.toString();
    }    
}
