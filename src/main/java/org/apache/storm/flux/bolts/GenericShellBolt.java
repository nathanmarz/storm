package org.apache.storm.flux.bolts;

import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;

import java.util.Map;

public class GenericShellBolt extends ShellBolt implements IRichBolt{

    public GenericShellBolt(String[] command){
        super(command);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
