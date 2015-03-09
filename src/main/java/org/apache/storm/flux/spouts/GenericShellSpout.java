package org.apache.storm.flux.spouts;

import backtype.storm.spout.ShellSpout;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;

import java.util.Map;

public class GenericShellSpout extends ShellSpout implements IRichSpout {

    public GenericShellSpout(String[] args){
        System.out.println("Creating GenericShellSpout with args: " + args);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
