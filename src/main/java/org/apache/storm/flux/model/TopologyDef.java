package org.apache.storm.flux.model;

import backtype.storm.generated.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

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
    private static Logger LOG = LoggerFactory.getLogger(TopologyDef.class);

    private String name;
    private Map<String, BeanDef> componentMap = new LinkedHashMap<String, BeanDef>(); // not required
    private Map<String, BoltDef> boltMap = new LinkedHashMap<String, BoltDef>();
    private Map<String, SpoutDef> spoutMap = new LinkedHashMap<String, SpoutDef>();
    private Map<String, Object> config = new HashMap<String, Object>();
    private List<StreamDef> streams = new ArrayList<StreamDef>();
    private List<IncludeDef> includes;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setName(String name, boolean override){
        if(this.name == null || override){
            this.name = name;
        } else {
            LOG.warn("Ignoring attempt to set property 'name' with override == false.");
        }
    }

    public List<SpoutDef> getSpouts() {
        ArrayList<SpoutDef> retval = new ArrayList<SpoutDef>();
        retval.addAll(this.spoutMap.values());
        return retval;
    }

    public void setSpouts(List<SpoutDef> spouts) {
        this.spoutMap = new LinkedHashMap<String, SpoutDef>();
        for(SpoutDef spout : spouts){
            this.spoutMap.put(spout.getId(), spout);
        }
    }

    public List<BoltDef> getBolts() {
        ArrayList<BoltDef> retval = new ArrayList<BoltDef>();
        retval.addAll(this.boltMap.values());
        return retval;
    }

    public void setBolts(List<BoltDef> bolts) {
        this.boltMap = new LinkedHashMap<String, BoltDef>();
        for(BoltDef bolt : bolts){
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
        ArrayList<BeanDef> retval = new ArrayList<BeanDef>();
        retval.addAll(this.componentMap.values());
        return retval;
    }

    public void setComponents(List<BeanDef> components) {
        this.componentMap = new LinkedHashMap<String, BeanDef>();
        for(BeanDef component : components){
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
        return this.boltMap.get(boltId).getParallelism();
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

    // used by includes implementation
    public void addAllBolts(List<BoltDef> bolts, boolean override){
        //TODO respect override
        for(BoltDef bolt : bolts){
            this.boltMap.put(bolt.getId(), bolt);
        }
    }

    public void addAllSpouts(List<SpoutDef> spouts, boolean override){
        //TODO respect override
        for(SpoutDef spout : spouts){
            this.spoutMap.put(spout.getId(), spout);
        }
    }

    public void addAllComponents(List<BeanDef> components, boolean override) {
        //TODO respect override
        for(BeanDef bean : components){
            this.componentMap.put(bean.getId(), bean);
        }
    }

    public void addAllStreams(List<StreamDef> streams, boolean override) {
        //TODO respect override
        this.streams.addAll(streams);
    }
}
