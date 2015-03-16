package org.apache.storm.flux.model;

import backtype.storm.generated.Bolt;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Bean represenation of a topology.
 *
 * It consists of the following:
 *   1. The topology name
 *   2. A `java.util.Map` representing the `backtype.storm.config` for the topology
 *   3. A list of spout definitions
 *   4. A list of bolt definitions
 *   5. A list of stream definitions that define the flow between spouts and bolts.
 *
 */
public class TopologyDef {

    private String name;
    private List<BeanDef> components;
    private Map<String, BeanDef> componentMap;
    private Map<String, BoltDef> boltMap;
    private Map<String, SpoutDef> spoutMap;
    private Map<String, Object> config;
    private List<SpoutDef> spouts;
    private List<BoltDef> bolts;
    private List<StreamDef> streams;
    private List<IncludeDef> includes;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<SpoutDef> getSpouts() {
        return spouts;
    }

    public void setSpouts(List<SpoutDef> spouts) {
        this.spouts = spouts;
        this.spoutMap = new HashMap<String, SpoutDef>();
        for(SpoutDef spout : this.spouts){
            this.spoutMap.put(spout.getId(), spout);
        }
    }

    public List<BoltDef> getBolts() {
        return bolts;
    }

    public void setBolts(List<BoltDef> bolts) {
        this.bolts = bolts;
        this.boltMap = new HashMap<String, BoltDef>();
        for(BoltDef bolt : this.bolts){
            this.boltMap.put(bolt.getId(), bolt);
        }
    }

    public List<StreamDef> getStreams() {
        return streams;
    }

    public void setStreams(List<StreamDef> streams) {
        this.streams = streams;
    }

    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }

    public List<BeanDef> getComponents() {
        return components;
    }

    public void setComponents(List<BeanDef> components) {
        this.components = components;
        this.componentMap = new HashMap<String, BeanDef>();
        for(BeanDef component : this.components){
            this.componentMap.put(component.getId(), component);
        }
    }

    public List<IncludeDef> getIncludes() {
        return includes;
    }

    public void setIncludes(List<IncludeDef> includes) {
        this.includes = includes;
    }

    // utility methods
    public int parallelismForBolt(String boltId){
        for(BoltDef bd : this.bolts){
            if(bd.getId().equals(boltId)){
                return bd.getParallelism();
            }
        }
        return -1;
    }

    public BoltDef getBoltDef(String id){
        return this.boltMap.get(id);
    }

    public SpoutDef getSpoutDef(String id){
        return this.spoutMap.get(id);
    }

    public BeanDef getComponent(String id){
        return this.componentMap.get(id);
    }
}
