package org.apache.storm.flux.spouts;

import backtype.storm.spout.ShellSpout;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

import java.util.Map;

public class GenericShellSpout extends ShellSpout implements IRichSpout {
    private String[] outputFields;

    public GenericShellSpout(String[] args, String[] outputFields){
        super(args);
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
