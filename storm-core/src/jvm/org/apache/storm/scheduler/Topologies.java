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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.scheduler.resource.Component;

public class Topologies {
    Map<String, TopologyDetails> topologies;
    Map<String, String> nameToId;
    Map<String, Map<String, Component>> _allComponents;

    public Topologies(Map<String, TopologyDetails> topologies) {
        if(topologies==null) topologies = new HashMap<>();
        this.topologies = new HashMap<>(topologies.size());
        this.topologies.putAll(topologies);
        this.nameToId = new HashMap<>(topologies.size());
        
        for (Map.Entry<String, TopologyDetails> entry : topologies.entrySet()) {
            TopologyDetails topology = entry.getValue();
            this.nameToId.put(topology.getName(), entry.getKey());
        }
    }

    /**
     * copy constructor
     */
    public Topologies(Topologies src) {
        this(src.topologies);
    }
    
    public TopologyDetails getById(String topologyId) {
        return this.topologies.get(topologyId);
    }
    
    public TopologyDetails getByName(String topologyName) {
        String topologyId = this.nameToId.get(topologyName);
        
        if (topologyId == null) {
            return null;
        } else {
            return this.getById(topologyId);
        }
    }
    
    public Collection<TopologyDetails> getTopologies() {
        return this.topologies.values();
    }

    public Map<String, Map<String, Component>> getAllComponents() {
        if (_allComponents == null) {
            _allComponents = new HashMap<>();
            for (Map.Entry<String, TopologyDetails> entry : this.topologies.entrySet()) {
                _allComponents.put(entry.getKey(), entry.getValue().getComponents());
            }
        }
        return _allComponents;
    }

    @Override
    public String toString() {
        StringBuilder ret = new StringBuilder();
        ret.append("Topologies:\n");
        for (TopologyDetails td : this.getTopologies()) {
            ret.append(td.toString()).append("\n");
        }
        return ret.toString();
    }
}
