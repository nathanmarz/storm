package org.apache.storm.flux.model;

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
    private Map<String, Object> config;
    private List<SpoutDef> spouts;
    private List<BoltDef> bolts;
    private List<StreamDef> streams;

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
    }

    public List<BoltDef> getBolts() {
        return bolts;
    }

    public void setBolts(List<BoltDef> bolts) {
        this.bolts = bolts;
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

    // utility methods
    public int parallelismForBolt(String boltId){
        for(BoltDef bd : this.bolts){
            if(bd.getId().equals(boltId)){
                return bd.getParallelism();
            }
        }
        return -1;
    }

    public BeanDef getComponent(String id){
        return this.componentMap.get(id);
    }
}
