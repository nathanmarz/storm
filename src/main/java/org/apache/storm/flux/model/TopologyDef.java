package org.apache.storm.flux.model;

import java.util.List;
import java.util.Map;

public class TopologyDef {

    private String name;

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

    // utility methods
    public int parallelismForBolt(String boltId){
        for(BoltDef bd : this.bolts){
            if(bd.getId().equals(boltId)){
                return bd.getParallelism();
            }
        }
        return -1;
    }
}
