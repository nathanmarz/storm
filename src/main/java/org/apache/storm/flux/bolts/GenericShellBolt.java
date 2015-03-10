package org.apache.storm.flux.bolts;

import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

import java.util.Map;

public class GenericShellBolt extends ShellBolt implements IRichBolt{
    private String[] outputFields;

    public GenericShellBolt(String[] command, String[] outputFields){
        super(command);
        this.outputFields = outputFields;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(this.outputFields));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
