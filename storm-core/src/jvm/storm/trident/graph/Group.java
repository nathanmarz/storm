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
package storm.trident.graph;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.jgrapht.DirectedGraph;
import storm.trident.planner.Node;
import storm.trident.util.IndexedEdge;
import storm.trident.util.TridentUtils;


public class Group {
    public Set<Node> nodes = new HashSet<Node>();
    private DirectedGraph<Node, IndexedEdge> graph;
    private String id;
    
    public Group(DirectedGraph graph, List<Node> nodes) {
        init(graph);
        this.nodes.addAll(nodes);
        this.graph = graph;
    }
    
    public Group(DirectedGraph graph, Node n) {
        this(graph, Arrays.asList(n));
    }
    
    public Group(Group g1, Group g2) {
        init(g1.graph);
        nodes.addAll(g1.nodes);
        nodes.addAll(g2.nodes);
    }
    
    private void init(DirectedGraph graph) {
        this.graph = graph;
        this.id = UUID.randomUUID().toString();
    }
    
    public Set<Node> outgoingNodes() {
        Set<Node> ret = new HashSet<Node>();
        for(Node n: nodes) {
            ret.addAll(TridentUtils.getChildren(graph, n));
        }
        return ret;
    }
    
    public Set<Node> incomingNodes() {
        Set<Node> ret = new HashSet<Node>();
        for(Node n: nodes) {
            ret.addAll(TridentUtils.getParents(graph, n));
        }        
        return ret;        
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return id.equals(((Group) o).id);
    }

    @Override
    public String toString() {
        return nodes.toString();
    }    
}
