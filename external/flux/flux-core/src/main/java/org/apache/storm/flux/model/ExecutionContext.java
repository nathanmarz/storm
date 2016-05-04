/*
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
package org.apache.storm.flux.model;

import org.apache.storm.Config;
import org.apache.storm.task.IBolt;
import org.apache.storm.topology.IRichSpout;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Container for all the objects required to instantiate a topology.
 */
public class ExecutionContext {
    // parsed Topology definition
    TopologyDef topologyDef;

    // Storm config
    private Config config;

    // components
    private List<Object> compontents;
    // indexed by id
    private Map<String, Object> componentMap = new HashMap<String, Object>();

    private Map<String, IRichSpout> spoutMap = new HashMap<String, IRichSpout>();

    private List<IBolt> bolts;
    private Map<String, Object> boltMap = new HashMap<String, Object>();

    public ExecutionContext(TopologyDef topologyDef, Config config){
        this.topologyDef = topologyDef;
        this.config = config;
    }

    public TopologyDef getTopologyDef(){
        return this.topologyDef;
    }

    public void addSpout(String id, IRichSpout spout){
        this.spoutMap.put(id, spout);
    }

    public void addBolt(String id, Object bolt){
        this.boltMap.put(id, bolt);
    }

    public Object getBolt(String id){
        return this.boltMap.get(id);
    }

    public void addComponent(String id, Object value){
        this.componentMap.put(id, value);
    }

    public Object getComponent(String id){
        return this.componentMap.get(id);
    }

}
