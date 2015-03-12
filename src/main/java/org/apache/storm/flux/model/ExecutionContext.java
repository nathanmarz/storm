package org.apache.storm.flux.model;

import backtype.storm.Config;
import backtype.storm.task.IBolt;
import backtype.storm.topology.IRichSpout;

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
    // list
    private List<Object> compontents;
    // indexed by id
    private Map<String, Object> componentMap = new HashMap<String, Object>();

//    private List<IRichSpout> spouts;
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
